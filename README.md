# ros2_launch_helpers

`ros2_launch_helpers` provides small helpers for Python ROS 2 launch files.

## Purpose

This package helps launch files do a few common tasks:

- Declare compact launch arguments for optional action arguments.
- Use explicit launch actions to update common launch context values.
- Validate and transform namespaces and project name segments.
- Build robot namespaces, robot prefixes, and rendered parameter file paths.
- Parse launch action arguments from JSON strings and Python defaults.
- Convert remapping pairs into the tuple form expected by `launch_ros.actions.Node`.

## Launch actions

Use actions when a launch file needs to resolve launch substitutions, compute a new value, and write it back into the launch context. Each action handles evaluation during launch while delegating the underlying computation to a helper function, which can be tested without running a full launch description.

These are standard ROS 2 launch actions and can be added directly to the `LaunchDescription`. If creating them requires reading the current launch context first, create or return them from an `OpaqueFunction` callback.

```python
import ros2_launch_helpers as rlh
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.conditions import IfCondition
from launch.substitutions import LaunchConfiguration


def generate_launch_description():
    return LaunchDescription(
        [
            DeclareLaunchArgument('namespace', default_value=''),
            DeclareLaunchArgument('robot_name', default_value='robot_1'),
            DeclareLaunchArgument('params_file'),
            DeclareLaunchArgument('params_file_allow_substs', default_value='true'),
            rlh.SetGlobalNamespace(
                namespace=LaunchConfiguration('namespace'), output_context_key='namespace'
            ),
            rlh.SetRobotNamespace(
                namespace=LaunchConfiguration('namespace'),
                robot_name=LaunchConfiguration('robot_name'),
                output_context_key='robot_namespace',
            ),
            rlh.SetRobotPrefix(
                robot_name=LaunchConfiguration('robot_name'), output_context_key='robot_prefix'
            ),
            rlh.RequireFile(path=LaunchConfiguration('params_file')),
            rlh.RenderParamsFile(
                params_file=LaunchConfiguration('params_file'),
                output_context_key='params_file',
                condition=IfCondition(LaunchConfiguration('params_file_allow_substs')),
            ),
        ]
    )
```

Action inputs accept standard ROS 2 launch substitutions: a plain string supplies literal text, while `LaunchConfiguration('robot_name')` reads a launch argument or another value from the launch context. Output context key arguments also accept substitutions, but must resolve to a non-empty launch configuration key.

- `SetGlobalNamespace(namespace=..., output_context_key=...)` resolves one namespace value and writes the absolute namespace to the resolved output context key.
- `SetRobotNamespace(namespace=..., robot_name=..., output_context_key=...)` resolves a parent namespace and robot name, then writes the combined namespace to the resolved output context key.
- `SetRobotPrefix(robot_name=..., output_context_key=...)` resolves a robot name, then writes the robot prefix to the resolved output context key.
- `RequireDirectory(path=...)` resolves a filesystem path and requires it to be an existing directory.
- `RequireFile(path=...)` resolves a filesystem path and requires it to be an existing file.
- `RenderParamsFile(params_file=..., output_context_key=...)` resolves a filesystem path, renders the file using the current launch context, and writes the rendered path to the resolved output context key. The temporary file remains available after launch shutdown, and the standard launch `condition` argument can restrict rendering to selected launch configurations.

`ProcessParamsFile` prepares a single chosen file according to an explicit rendering flag:

```python
rlh.ProcessParamsFile(
    params_file=LaunchConfiguration('robot_params_file'),
    allow_substs=LaunchConfiguration('robot_params_file_allow_substs'),
    output_context_key='resolved_robot_params_file',
)
```

The selected file must exist regardless of whether rendering is enabled. The `allow_substs` flag accepts a Python boolean or launch substitutions evaluated with ROS launch's typed boolean conversion, which rejects invalid values. When enabled, the action returns `RenderParamsFile`; otherwise, it returns `SetLaunchConfiguration` to publish the original path.

Because the flag controls rendering independently of the filename, debug entry points can accept package-owned or external files without requiring a directory convention. Downstream consumers should use the output path with further rendering disabled to avoid processing substitutions twice. The standard launch `condition` argument can disable the action entirely.

`ResolveParamsFile` selects between a direct parameter YAML and a template:

```python
rlh.ResolveParamsFile(
    params_file=PathJoinSubstitution([LaunchConfiguration('robot_dir'), 'params.yaml']),
    template_params_file=PathJoinSubstitution(
        [LaunchConfiguration('robot_dir'), 'params.template.yaml']
    ),
    output_context_key='robot_params_file',
)
```

The example requires importing `PathJoinSubstitution` from `launch.substitutions`, but its filenames and directory layout are only illustrative: callers supply both candidate paths explicitly. Both paths and the output key accept launch substitutions. Exactly one candidate must exist and be a regular file; empty inputs, directories, and cases where both or neither candidate exists cause an error.

Once the source is selected, the action returns one child action for launch to execute: `SetLaunchConfiguration` publishes the direct file path, while `RenderParamsFile` renders the template and publishes the generated path. Subsequent actions read that path through `LaunchConfiguration('robot_params_file')` with further rendering disabled. The standard launch `condition` argument can disable selection entirely, and `RenderParamsFile` remains available for unconditional rendering.

The lower-level helpers remain available for code that already has concrete values:

```python
absolute_namespace = rlh.make_namespace_absolute('robots')
robot_namespace = rlh.make_robot_namespace(absolute_namespace, 'front')
robot_prefix = rlh.make_robot_prefix('front')
```

## Launch action arguments

Use `bridge_arguments_json_str`, `speed_controller_arguments_json_str`, or a similar launch argument when a launch file should let the application configure optional fields of one `Node`, `ExecuteProcess`, or `ExecuteLocal` action.

Declaring a separate launch argument for every optional field makes launch files harder to read as the number of nodes and processes grows. With this helper, each configurable action instead receives its own JSON string containing the action arguments directly, without an enclosing key such as `"bridge"`.

The launch file author can also provide `default_arguments` as Python values, which the helper validates before returning the resolved arguments. When both the JSON object and `default_arguments` define a field, the JSON value takes precedence.

Keep fields such as `package`, `executable`, `parameters`, and `cmd` explicit in Python so that readers can see what the launch file starts and how it is configured. The rationale is explained in [Launch action arguments technical design](doc/launch_action_arguments_design.md).

Resolve the JSON string where a `LaunchContext` is available, typically inside an `OpaqueFunction` callback. The callback can then pass the resolved arguments to the action with `**bridge_arguments`, as shown below.

```python
import ros2_launch_helpers as rlh
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def launch_setup(ctx):
    bridge_arguments = rlh.resolve_node_arguments(
        LaunchConfiguration('bridge_arguments_json_str').perform(ctx),
        default_arguments={'name': 'bridge', 'output': 'screen', 'emulate_tty': True},
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
    return LaunchDescription(
        [
            DeclareLaunchArgument('robot_namespace', default_value=''),
            DeclareLaunchArgument(
                'bridge_arguments_json_str',
                default_value=rlh.default_launch_action_arguments_json_str(),
                description=rlh.LAUNCH_ACTION_ARGUMENTS_DESC,
            ),
            OpaqueFunction(function=launch_setup),
        ]
    )
```

Each launch file supplies `default_arguments` for the action it creates; the helper does not apply global defaults.

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

The supported fields follow the constructors of `Node`, `ExecuteProcess`, and `ExecuteLocal`. JSON overrides must use JSON-compatible values, while Python `default_arguments` follow the same value shapes where possible. Fields that are optional in the original ROS 2 constructor also accept `null` in JSON or `None` in Python.

From `launch_ros.actions.Node`:

- `name`: the ROS node name; a string is preferred, but `list[string]` and null are also accepted.
- `exec_name`: the launch process label; a string is preferred, but `list[string]` and null are also accepted.
- `namespace`: string preferred, `list[string]` and null accepted.
- `remappings`: a list of two-item lists or null. In Python `default_arguments`, each pair may also be a tuple, for example `[('from', 'to')]`; the helper converts all pairs into tuples for `Node`.
- `ros_arguments`: `list[string]` or null.
- `arguments`: `list[string]` or null.

From `launch.actions.ExecuteProcess`:

- `name`: the launch process label; a string is preferred, but `list[string]` and null are also accepted.
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

The [technical design document](doc/launch_action_arguments_design.md) provides the full rationale and lists the ROS 2 source files used to define the supported fields.

## SomeSubstitutionsType values

Fields that accept ROS 2 launch `SomeSubstitutionsType` can receive a JSON string or a list of strings. Prefer a single string for readability, since the helper only concatenates list elements. Neither form can represent launch `Substitution` objects such as `LaunchConfiguration` or `FindPackageShare`.

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

Concatenation does not insert separators: for example, `["2", "5"]` becomes `"25"` seconds.

## Name and exec name

The `name` field has the same meaning it would have if written directly in Python launch code.

When arguments are passed to `Node`, `name` is the ROS node name and `exec_name` is the launch process label forwarded to `ExecuteProcess.name`.

When arguments are passed directly to `ExecuteProcess`, `name` is the launch process label and `exec_name` is not accepted.

## When to use

- When a launch file should accept optional launch action arguments without declaring one launch argument per action argument.
- When parent launch files should be able to pass process arguments or remappings to included launch files.
- When a launch file creates one or more actions and needs processed arguments in the shape expected by ROS 2 launch.

## License

This package is distributed under the Apache License 2.0; see [LICENSE](LICENSE) for the complete terms.
