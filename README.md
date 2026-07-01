# ros2_launch_helpers

`ros2_launch_helpers` provides small helpers for Python ROS 2 launch files.

## Purpose

This package helps launch files do a few common tasks:

- Declare compact launch arguments for action arguments, parameter files, overlays, and namespaces.
- Use explicit launch actions to update common launch context values.
- Compute robot namespaces, robot prefixes, and rendered parameter file paths.
- Build parameter layers from a base file and additional overlays.
- Parse launch action arguments from JSON strings and Python defaults.
- Convert remapping pairs into the tuple form expected by `launch_ros.actions.Node`.

## Launch actions

Use actions when a launch file needs to read a value from the launch context, compute a new value, and write that new value back into the launch context. The action owns the launch-context part. A helper function owns the simple computation. This keeps the computation easy to test without running a full launch description.

The actions are normal ROS 2 launch actions. Put them in the `LaunchDescription`. If the launch file first needs to read the current launch context, create or return them from an `OpaqueFunction` callback.

```python
import ros2_launch_helpers as rlh
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument('namespace', default_value=''),
        DeclareLaunchArgument('robot_name', default_value='robot_1'),
        DeclareLaunchArgument('params_file'),
        DeclareLaunchArgument('params_file_allow_substs', default_value='true'),
        rlh.SetGlobalNamespace(namespace=LaunchConfiguration('namespace'), output_namespace_key='namespace'),
        rlh.SetRobotNamespace(
            namespace=LaunchConfiguration('namespace'),
            robot_name=LaunchConfiguration('robot_name'),
            robot_namespace_key='robot_namespace',
        ),
        rlh.SetRobotPrefix(robot_name=LaunchConfiguration('robot_name'), robot_prefix_key='robot_prefix'),
        rlh.ProcessParamsFile(
            params_file=LaunchConfiguration('params_file'),
            allow_substs=LaunchConfiguration('params_file_allow_substs'),
            output_params_file_key='params_file',
        ),
    ])
```

Action inputs that read launch context values accept two forms. They can receive a launch configuration name string, such as `'robot_name'`. They can also receive a `LaunchConfiguration` object, such as `LaunchConfiguration('robot_name')`. Passing `LaunchConfiguration(...)` makes the runtime lookup explicit at the call site.

- `SetGlobalNamespace()` reads `namespace` and writes the absolute namespace back to `namespace`.
- `SetRobotNamespace()` reads `namespace` and `robot_name`, then writes `robot_namespace`.
- `SetRobotPrefix()` reads `robot_name`, then writes `robot_prefix`.
- `ProcessParamsFile()` reads a resolved filesystem path from `params_file` and reads `params_file_allow_substs`, then writes the same path or the rendered path back to `params_file`.

The lower-level helpers remain available for code that already has concrete values:

```python
robot_namespace = rlh.compute_robot_namespace('/robots', 'front')
robot_prefix = rlh.compute_robot_prefix('front')
```

## Launch action arguments

Use `bridge_arguments_json_str`, `speed_controller_arguments_json_str`, or a similar launch argument when a launch file should let the application configure optional fields of one `Node`, `ExecuteProcess`, or `ExecuteLocal` action.

Without this helper, the launch file would need one launch argument for every optional field. That becomes hard to read when a launch file starts several nodes or processes.

With this helper, each configurable action receives its own JSON string. The JSON object contains the arguments for that action directly. There is no extra key such as `"bridge"` inside the JSON.

The launch file may also provide `default_arguments`. Defaults are normal Python values written by the launch file author. They are validated by the same helper before they are returned. If the JSON object and `default_arguments` define the same field, the JSON value wins.

Some fields should still stay in the Python launch file. For example, the launch file should normally keep `package`, `executable`, `parameters`, and `cmd` visible in Python code. For the longer explanation, see [Launch action arguments technical design](doc/launch_action_arguments_design.md).

Resolve the JSON string inside code that has a `LaunchContext`. A common pattern is to do it inside an `OpaqueFunction` callback. The callback reads the JSON string and passes the returned arguments to the action with `**bridge_arguments`.

```python
import ros2_launch_helpers as rlh
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def launch_setup(ctx):
    bridge_arguments = rlh.resolve_node_arguments(
        LaunchConfiguration('bridge_arguments_json_str').perform(ctx),
        default_arguments={
            'name': 'bridge',
            'output': 'screen',
            'emulate_tty': True,
        },
    )

    return [
        Node(
            package='ros_gz_bridge',
            executable='bridge_node',
            namespace=LaunchConfiguration('robot_namespace'),
            **bridge_arguments,
        )
    ]


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument('robot_namespace', default_value=''),
        DeclareLaunchArgument(
            'bridge_arguments_json_str',
            default_value=rlh.default_launch_action_arguments_json_str(),
            description=rlh.LAUNCH_ACTION_ARGUMENTS_DESC,
        ),
        OpaqueFunction(function=launch_setup),
    ])
```

The helper applies no global `default_arguments`. Each launch file supplies `default_arguments` for the action it is creating.

## JSON format

The default launch argument value is `"{}"`, which means "there are no overrides for this action".

Example CLI override:

```bash
ros2 launch my_robot bringup.launch.py \
  bridge_arguments_json_str:='{"output":"screen","respawn":true,"respawn_delay":2.0}'
```

Example `bridge_arguments_json_str` value:

```json
{
  "name": "robot_bridge",
  "output": "screen",
  "emulate_tty": true,
  "respawn": true,
  "respawn_delay": 2.0,
  "ros_arguments": ["--log-level", "debug"],
  "remappings": [
    ["battery_state", "state/battery"],
    ["cmd_vel", "commands/velocity"]
  ],
  "additional_env": {
    "RCUTILS_COLORIZED_OUTPUT": "1"
  }
}
```

## Supported fields

The supported fields come from `Node`, `ExecuteProcess`, and `ExecuteLocal`. The JSON string must use JSON-compatible values. `default_arguments` uses Python values, but it follows the same value shapes where possible. When a field is optional in the original ROS 2 constructor, `null` in JSON or `None` in Python is accepted.

From `launch_ros.actions.Node`:

- `name`: string preferred, `list[string]` and null accepted. For `Node`, this is the ROS node name.
- `exec_name`: string preferred, `list[string]` and null accepted. For `Node`, this is forwarded as the launch process label.
- `namespace`: string preferred, `list[string]` and null accepted.
- `remappings`: list of two-item lists or null. In `default_arguments`, each pair may also be a tuple, for example `[('from', 'to')]`. The helper converts each pair into a tuple for `Node`.
- `ros_arguments`: `list[string]` or null.
- `arguments`: `list[string]` or null.

From `launch.actions.ExecuteProcess`:

- `name`: string preferred, `list[string]` and null accepted. For `ExecuteProcess`, this is the launch process label.
- `prefix`: string preferred, `list[string]` and null accepted.
- `cwd`: string preferred, `list[string]` and null accepted.
- `env`: object with string keys and string values, or null.
- `additional_env`: object with string keys and string values, or null.

From `launch.actions.ExecuteLocal`:

- `shell`: boolean.
- `sigterm_timeout`: string preferred, `list[string]` accepted.
- `sigkill_timeout`: string preferred, `list[string]` accepted.
- `emulate_tty`: boolean.
- `output`: string preferred, `list[string]` accepted.
- `output_format`: string.
- `cached_output`: boolean.
- `log_cmd`: boolean.
- `respawn`: boolean.
- `respawn_delay`: number or null.
- `respawn_max_retries`: integer.

The helper rejects fields that should stay explicit in the launch file:

- `package`
- `executable`
- `parameters`
- `cmd`
- `process_description`
- `on_exit`
- `condition`

See [doc/launch_action_arguments_design.md](doc/launch_action_arguments_design.md) for the full design notes. That document also lists the ROS 2 source files used to define this supported field list.

## SomeSubstitutionsType values

When a field accepts ROS 2 launch `SomeSubstitutionsType`, the JSON value may be a string or a list of strings. Prefer the string form. JSON cannot represent launch `Substitution` objects such as `LaunchConfiguration` or `FindPackageShare`. In this helper, the list form only concatenates strings, so it is usually harder to read.

Preferred:

```json
{
  "sigterm_timeout": "2.5"
}
```

Accepted:

```json
{
  "sigterm_timeout": ["2", ".", "5"]
}
```

`list[string]` is concatenated. For example, `["2", "5"]` becomes `"25"` seconds.

## Name and exec name

The `name` field has the same meaning it would have if written directly in Python launch code.

When arguments are passed to `Node`, `name` is the ROS node name and `exec_name` is the launch process label forwarded to `ExecuteProcess.name`.

When arguments are passed directly to `ExecuteProcess`, `name` is the launch process label and `exec_name` is not accepted.

## When to use

- When a launch file should accept optional launch action arguments without declaring one launch argument per action argument.
- When parent launch files should be able to pass process arguments or remappings to included launch files.
- When a launch file creates one or more actions and needs processed arguments in the shape expected by ROS 2 launch.
