# Launch action arguments technical design

## Problem and goal

ROS 2 launch files often create actions with two different kinds of fields. Some fields describe what the launch file is going to start. Other fields only change how that action is started. For a `Node`, the launch file usually knows the `package`, the `executable`, the namespace rule, and the parameter files or parameter dictionaries. Those fields are part of the package design. They say which node is launched and how that node is connected to the rest of the launch system. Other fields are different. Fields such as `name`, `exec_name`, `remappings`, `ros_arguments`, `arguments`, `prefix`, `cwd`, `env`, `additional_env`, `output`, `emulate_tty`, `respawn`, and timeout fields do not normally change which node is launched. They change details of how the already selected action runs.

The list of possible fields becomes clearer when looking at the constructors. `launch_ros.actions.Node` defines fields that are specific to a ROS node. It also accepts `**kwargs`. Those extra keyword arguments are passed to its parent class.

```python
class Node(ExecuteProcess):
    def __init__(
        self,
        *,
        executable: SomeSubstitutionsType,
        package: Optional[SomeSubstitutionsType] = None,
        name: Optional[SomeSubstitutionsType] = None,
        namespace: Optional[SomeSubstitutionsType] = None,
        exec_name: Optional[SomeSubstitutionsType] = None,
        parameters: Optional[SomeParameters] = None,
        remappings: Optional[SomeRemapRules] = None,
        ros_arguments: Optional[Iterable[SomeSubstitutionsType]] = None,
        arguments: Optional[Iterable[SomeSubstitutionsType]] = None,
        **kwargs,
    ) -> None: ...
```

`Node` inherits from `launch.actions.ExecuteProcess`. That means a `Node` can also receive the fields accepted by `ExecuteProcess`. `ExecuteProcess` inherits from `launch.actions.ExecuteLocal`. That means a `Node` can also receive the fields accepted by `ExecuteLocal`.

```python
class ExecuteProcess(ExecuteLocal):
    def __init__(
        self,
        *,
        cmd: Iterable[SomeSubstitutionsType],
        prefix: Optional[SomeSubstitutionsType] = None,
        name: Optional[SomeSubstitutionsType] = None,
        cwd: Optional[SomeSubstitutionsType] = None,
        env: Optional[Dict[SomeSubstitutionsType, SomeSubstitutionsType]] = None,
        additional_env: Optional[Dict[SomeSubstitutionsType, SomeSubstitutionsType]] = None,
        **kwargs,
    ) -> None: ...
```

`ExecuteLocal` adds fields that control how the process is executed on the local machine. These fields control things such as shell usage, termination timeouts, output handling, command logging, exit handlers, and respawn behavior.

```python
class ExecuteLocal(Action):
    def __init__(
        self,
        *,
        process_description: Executable,
        shell: bool = False,
        sigterm_timeout: SomeSubstitutionsType = LaunchConfiguration('sigterm_timeout', default=5),
        sigkill_timeout: SomeSubstitutionsType = LaunchConfiguration('sigkill_timeout', default=5),
        emulate_tty: bool = False,
        output: SomeSubstitutionsType = 'log',
        output_format: Text = '[{this.process_description.final_name}] {line}',
        cached_output: bool = False,
        log_cmd: bool = False,
        on_exit: Optional[
            Union[
                SomeEntitiesType,
                Callable[[ProcessExited, LaunchContext], Optional[SomeEntitiesType]],
            ]
        ] = None,
        respawn: bool = False,
        respawn_delay: Optional[float] = None,
        respawn_max_retries: int = -1,
        **kwargs,
    ) -> None: ...
```

The important consequence is simple: when a launch file creates a `Node`, the launch file author is not only choosing values for the `Node` constructor. The author is also deciding which fields from `ExecuteProcess` and `ExecuteLocal` can be configured by the user of that launch file.

The direct ROS 2 launch solution is to expose configurable values with `DeclareLaunchArgument`. That is a good solution when the launch file only needs a few clear inputs. It becomes harder to use when a launch file starts several actions and each action may need several optional fields. If a launch file starts one node, exposing every possible `Node`, `ExecuteProcess`, and `ExecuteLocal` field would already create many launch arguments. If a launch file starts three nodes, those launch arguments have to be repeated for each node or replaced with custom parsing code. Both choices make the launch file harder to read. They also make it harder for the application user to know which values can be overridden.

A helper API should not expose only a few selected fields and then need a new helper argument every time another valid launch field becomes useful. That kind of API is easy to start with, but it becomes difficult to grow in a consistent way. For example, a user may later need a valid but less common field such as `prefix`, `cwd`, `additional_env`, `arguments`, or `cached_output`. The package should not need a new dedicated setting for each one of those fields.

The goal of this helper is to support one structured launch argument for the optional fields of one action. The launch argument value is a JSON string. The helper reads that JSON string, checks that the field names and value types are supported, and returns one normal Python dictionary. The launch file can then pass that dictionary to `Node`, `ExecuteProcess`, or `ExecuteLocal` with Python `**kwargs` syntax.

The JSON value is not meant to replace the Python launch file. The launch file still writes the fields that decide what is being launched. Examples are `Node.package`, `Node.executable`, `Node.parameters`, and `ExecuteProcess.cmd`. Those fields are important because they show the reader which package, executable, parameters, or command the launch file uses. They should stay visible in the Python launch code. The JSON object is only for optional fields that adjust an action that the launch file has already chosen. `Node.namespace` is allowed by this helper because some launch files may choose to expose the already-resolved namespace as an action argument.

Each configurable action should receive its own launch argument. For example, a launch file can expose `bridge_arguments_json_str` for a bridge node and `speed_controller_arguments_json_str` for a controller node. Each JSON string contains the arguments for that one action directly. There is no extra lookup key inside the JSON object.

```json
{
  "name": "robot_bridge",
  "output": "screen",
  "emulate_tty": true,
  "respawn": true,
  "remappings": [
    ["battery_state", "state/battery"],
    ["cmd_vel", "commands/velocity"]
  ]
}
```

In this example, the JSON object can be used as the value of `bridge_arguments_json_str`. The launch file decides that this launch argument belongs to the bridge action. The JSON does not need to repeat that decision with another key named `bridge`.


## Design approach

The helper chooses a middle point between two designs.

One possible design is to create one `DeclareLaunchArgument` for every optional field. That makes every value explicit. The problem is that it creates too many launch arguments. It becomes especially hard to use when the same launch file starts several nodes or processes.

Another possible design is to accept any JSON object and pass it directly to the action without checking it. That would be flexible, but it would make mistakes harder to find. A typo such as `"emulate_ttyy"` would not be detected by the helper. The error would appear later, usually inside ROS 2 launch, and the message would be less connected to the original JSON input.

The chosen design is strict, but only inside a clear boundary. The helper supports a known list of fields from `Node`, `ExecuteProcess`, and `ExecuteLocal`. Each supported field has a documented JSON shape. Unknown fields fail early. Fields that should stay in the launch file also fail early. Each resolver checks the action level it is meant for. `resolve_node_arguments(...)` accepts allowed `Node`, `ExecuteProcess`, and `ExecuteLocal` fields. `resolve_execute_process_arguments(...)` accepts allowed `ExecuteProcess` and `ExecuteLocal` fields, but rejects `Node` fields such as `remappings` or `ros_arguments`. `resolve_execute_local_arguments(...)` only accepts allowed `ExecuteLocal` and `Action` fields.

## Public API

The default launch argument value is:

```text
default_launch_action_arguments_json_str() -> str
```

It returns `"{}"`. That means the launch file receives an empty JSON object when the user does not override any action arguments.

The launch argument description is:

```python
LAUNCH_ACTION_ARGUMENTS_DESC
```

Use it as the `description` value when declaring an action-specific argument such as `bridge_arguments_json_str`.

The parser helper names are:

```text
resolve_execute_local_arguments(
    json_str_arguments: str | None,
    default_arguments: dict[str, Any] | None = None,
    extra_rejected_arguments: set[str] | None = None,
) -> dict[str, Any]

resolve_execute_process_arguments(
    json_str_arguments: str | None,
    default_arguments: dict[str, Any] | None = None,
    extra_rejected_arguments: set[str] | None = None,
) -> dict[str, Any]

resolve_node_arguments(
    json_str_arguments: str | None,
    default_arguments: dict[str, Any] | None = None,
    extra_rejected_arguments: set[str] | None = None,
) -> dict[str, Any]

resolve_remappings(
    argument_name: str,
    argument_value: object,
) -> list[tuple[str, str]] | None
```

Each `resolve_*_arguments(...)` helper parses arguments for one action class and its base classes. It can also receive default values from the launch file. When both `default_arguments` and the JSON object define the same field, the JSON value wins.

```text
default_arguments < JSON object
```

`extra_rejected_arguments` lets a launch file reject one more field for one specific action. The field must be known by that resolver. This is useful when a field is generally supported but should stay explicit in one launch file.

`resolve_remappings(...)` is public because remapping conversion is useful on its own. It validates remapping pairs and returns the tuple-pair format expected by `launch_ros.actions.Node`.

Example usage:

```python
bridge_arguments = rlh.resolve_node_arguments(
    LaunchConfiguration('bridge_arguments_json_str').perform(context),
    default_arguments={'name': 'bridge', 'output': 'screen', 'emulate_tty': True},
)

Node(
    package='ros_gz_bridge',
    executable='bridge_node',
    namespace=LaunchConfiguration('robot_namespace'),
    parameters=parameters,
    **bridge_arguments,
)
```


## What the helper does and does not do

Each helper reads the JSON string, validates `default_arguments`, and returns one argument dictionary. The launch file decides where that dictionary is used:

- `resolve_node_arguments(...)` returns arguments for `Node(..., **kwargs)`.
- `resolve_execute_process_arguments(...)` returns arguments for `ExecuteProcess(..., **kwargs)`.
- `resolve_execute_local_arguments(...)` returns arguments for `ExecuteLocal(..., **kwargs)`.

The helper checks that every field name is supported by the selected action class or one of its base classes, and that every value has the expected type. For example, `remappings` is supported by `resolve_node_arguments(...)` but rejected by `resolve_execute_process_arguments(...)`.

The helper should feel like writing the same fields directly in Python. For example, passing `exec_name` to `ExecuteProcess` is invalid in normal launch code. It is also invalid when `exec_name` arrives through this helper.

The launch file still writes the fields that define what is launched. Typical examples are:

- `Node` `package`
- `Node` `executable`
- `Node` `parameters`
- `ExecuteProcess` `cmd`

## JSON format

JSON is the preferred inline format for this `DeclareLaunchArgument` value. It is more verbose than a custom mini-syntax, but it has important advantages. It is standard. It can be written on the command line. It preserves booleans, numbers, lists, and objects. It also matches the style used by ROS 2 CLI commands such as `ros2 topic pub`.

A file-based companion can be added later for long configurations:

- `bridge_arguments_json_str`: JSON inline, useful for CLI overrides.
- `bridge_arguments_file`: YAML or JSON file, useful for project configuration.

## Source type references

The supported field list is based on the ROS 2 Jazzy Python files that define the original constructor types. These paths are useful when checking why a field is supported and which type ROS 2 launch expects:

- `SomeSubstitutionsType`: `/opt/ros/jazzy/lib/python3.12/site-packages/launch/some_substitutions_type.py`
- `launch_ros.actions.Node`: `/opt/ros/jazzy/lib/python3.12/site-packages/launch_ros/actions/node.py`
- `launch.actions.ExecuteProcess`: `/opt/ros/jazzy/lib/python3.12/site-packages/launch/actions/execute_process.py`
- `launch.actions.ExecuteLocal`: `/opt/ros/jazzy/lib/python3.12/site-packages/launch/actions/execute_local.py`

## SomeSubstitutionsType values

ROS 2 Jazzy defines `SomeSubstitutionsType` as:

```python
SomeSubstitutionsType = Union[Text, Path, Substitution, Iterable[Union[Text, Path, Substitution]]]
```

JSON cannot represent Python `Path` objects. JSON also cannot represent ROS 2 launch `Substitution` objects such as `LaunchConfiguration` or `FindPackageShare`. For fields typed as `SomeSubstitutionsType`, this helper supports only the part that can be written naturally in JSON. `default_arguments` follows the same rule so that defaults and JSON values use the same validation path:

- string
- list of strings

The string form is the recommended form. No current supported field is clearer as `list[string]`. The list form is accepted because ROS 2 launch accepts lists of substitutions. In this JSON helper, the list only joins strings together, so it is usually harder to read.

For each `SomeSubstitutionsType` field, the documentation shows the same value in both forms:

```json
{
  "sigterm_timeout": "2.5"
}
```

```json
{
  "sigterm_timeout": ["2", ".", "5"]
}
```

## Type and conversion rules

Most JSON values are passed through directly as action arguments. `default_arguments` uses Python values, but the accepted shapes are intentionally close to the JSON shapes. This keeps the rule simple: defaults and JSON use the same field policy, and JSON values override defaults.

For fields typed as `bool`, `float`, or `int`, the value must use the matching type directly. In JSON, a boolean must be written as `true` or `false`, not as `"true"` or `"false"`. A number must be written as `2.0`, not as `"2.0"`. In Python defaults, use `True`, `False`, integers, or floats directly. The helper does not convert strings into booleans or numbers. This keeps the behavior close to writing the same fields directly in Python.

For fields typed as `Optional[...]`, the JSON value may be `null`. A missing field and a field explicitly set to `null` are not written the same way, but both are valid. A missing field means the returned argument dictionary does not contain that field. A field set to `null` means the returned argument dictionary contains that field with Python `None` as its value.

There is one remapping conversion:

- `remappings`: JSON has no tuples, but `launch_ros.actions.Node` expects each remap rule as a tuple. JSON values use two-item lists: `[["from", "to"]]`. Python defaults may use two-item lists or two-item tuples: `[["from", "to"]]` or `[("from", "to")]`. The helper returns tuple pairs: `[("from", "to")]`.

Validation is strict for supported fields. The implementation has a field schema. That schema says which value shape is accepted for each field:

- `SomeSubstitutionsType` fields: string or `list[string]`.
- `Optional[SomeSubstitutionsType]` fields: string, `list[string]`, or null.
- `Optional[Iterable[SomeSubstitutionsType]]` fields: `list[string]` or null.
- `bool` fields: boolean.
- `int` fields: integer.
- `Optional[float]` fields: number or null.
- `env` and `additional_env`: object with string keys and string values, or null.
- `remappings`: list of two-item lists or tuples of strings, or null. Non-null values are converted to tuples after validation.

Unknown fields fail with a clear error. This is stricter than passing everything through blindly, but it catches mistakes earlier. It also keeps the documentation and the implementation aligned: if a field is accepted by the helper, it should be listed in this document.

Mutable defaults are copied after validation and conversion. This avoids a surprising case where mutating the returned argument dictionary also mutates the original `default_arguments` dictionary owned by the launch file. Values created from `json.loads(...)` are not copied again, because they are already fresh objects for that resolver call.

## Unsupported values

The launch action arguments object is not a second launch language. It only represents values that can be written in JSON. It does not try to represent any possible Python object, for example:

- `LaunchConfiguration`
- `PathJoinSubstitution`
- `FindPackageShare`
- `LogInfo`
- `IfCondition`
- `OpaqueFunction`
- callables

If a launch file needs those objects, it should create them in Python. The launch file can then combine those Python objects with the parsed arguments before creating the action.

## Name and exec name semantics

`name` does not mean the same thing for every action.

When arguments are passed to `Node`, `name` means the ROS node name:

```python
Node(package='demo_nodes_cpp', executable='talker', **{'name': 'talker'})
```

When arguments are passed to `Node`, `exec_name` means the launch process label. This is the name that launch uses for the process that is running the node. Internally, `Node` forwards `exec_name` as `ExecuteProcess.name`:

```python
Node(
    package='demo_nodes_cpp',
    executable='talker',
    **{'name': 'talker', 'exec_name': 'talker_process'},
)
```

When arguments are passed directly to `ExecuteProcess`, `name` means the launch process label. `ExecuteProcess` does not accept `exec_name`:

```python
ExecuteProcess(cmd=['gz', 'sim'], **{'name': 'gazebo_process'})
```

This matches normal Python launch behavior. The helper does not translate `exec_name` into `name` for `ExecuteProcess`. If a user wants to configure an `ExecuteProcess` name, the JSON field is `name`, not `exec_name`.

## README and this document

The README gives the short version needed to use the feature. It shows the launch argument, the helper functions, a JSON example, and the list of supported fields.

This document gives the longer explanation. It explains why the feature exists, which ROS 2 launch constructors it follows, why some fields are accepted, and why other fields are kept outside the JSON object.

Each supported field should have at least one JSON example. Fields backed by `SomeSubstitutionsType` should show the same value encoded in both supported forms:

- preferred form: string
- accepted form: list of strings

## JSON examples for supported fields

The examples in this section show the JSON content before shell quoting. When the value is passed on the command line, the whole JSON object is written as the value of an action-specific launch argument such as `bridge_arguments_json_str`.

### Node fields

This example shows every supported field that comes from `launch_ros.actions.Node`:

```json
{
  "name": "spd_controller",
  "exec_name": "spd_controller_process",
  "remappings": [
    ["battery_state", "state/battery"],
    ["cmd_vel", "commands/velocity"]
  ],
  "ros_arguments": ["--log-level", "debug"],
  "arguments": ["--controller-mode", "speed"]
}
```

For fields typed as `SomeSubstitutionsType`, the string form is preferred. The accepted list form writes the same final value as several string pieces:

```json
{
  "name": ["spd", "_controller"],
  "exec_name": ["spd", "_controller", "_process"]
}
```

### ExecuteProcess fields

This example shows every supported field that comes from `launch.actions.ExecuteProcess`:

```json
{
  "name": "gazebo_process",
  "prefix": "gdb -ex run --args",
  "cwd": "/tmp",
  "env": {
    "GAZEBO_RESOURCE_PATH": "/opt/my_robot/worlds"
  },
  "additional_env": {
    "RCUTILS_COLORIZED_OUTPUT": "1"
  }
}
```

The `name`, `prefix`, and `cwd` fields are typed as `SomeSubstitutionsType`, so they also accept the list form:

```json
{
  "name": ["gazebo", "_process"],
  "prefix": ["gdb", " -ex run --args"],
  "cwd": ["/", "tmp"]
}
```

### ExecuteLocal fields

This example shows every supported field that comes from `launch.actions.ExecuteLocal`:

```json
{
  "shell": false,
  "sigterm_timeout": "2.5",
  "sigkill_timeout": "10",
  "emulate_tty": true,
  "output": "screen",
  "output_format": "[{this.process_description.final_name}] {line}",
  "cached_output": false,
  "log_cmd": true,
  "respawn": true,
  "respawn_delay": 2.0,
  "respawn_max_retries": 3
}
```

The `sigterm_timeout`, `sigkill_timeout`, and `output` fields are typed as `SomeSubstitutionsType`, so they also accept the list form. The list form is joined into one string by ROS 2 launch:

```json
{
  "sigterm_timeout": ["2", ".", "5"],
  "sigkill_timeout": ["1", "0"],
  "output": ["scr", "een"]
}
```

The list form does not mean "several values". For example, `["1", "0"]` becomes `"10"`.

Optional fields can also be set to `null`. This is useful when the user wants to write explicitly that the Python value should be `None`:

```json
{
  "name": null,
  "remappings": null,
  "ros_arguments": null,
  "prefix": null,
  "env": null,
  "respawn_delay": null
}
```


## Supported fields reference

### From launch_ros.actions.Node

- `name: Optional[SomeSubstitutionsType] = None`
  - JSON: string preferred. `list[string]` and null accepted.
  - Meaning for `Node`: ROS node name.
- `exec_name: Optional[SomeSubstitutionsType] = None`
  - JSON: string preferred. `list[string]` and null accepted.
  - Meaning for `Node`: launch process label forwarded as `ExecuteProcess.name`.
- `namespace: Optional[SomeSubstitutionsType] = None`
  - JSON: string preferred. `list[string]` and null accepted.
  - Meaning for `Node`: namespace passed directly to the `Node` action.
- `remappings: Optional[SomeRemapRules] = None`
  - JSON: list of two-item lists, for example `[["from", "to"]]`, or null.
  - Python defaults: list of two-item lists or two-item tuples, for example `[("from", "to")]`.
  - The helper converts each pair to a tuple before forwarding the arguments.
- `ros_arguments: Optional[Iterable[SomeSubstitutionsType]] = None`
  - JSON: `list[string]` or null.
- `arguments: Optional[Iterable[SomeSubstitutionsType]] = None`
  - JSON: `list[string]` or null.

Rejected from `launch_ros.actions.Node`:

- `package: Optional[SomeSubstitutionsType] = None`
  - Reason: `package` says which ROS package provides the node. The launch file should keep that visible.
- `executable: SomeSubstitutionsType`
  - Reason: `executable` says which binary is launched. The launch file should keep that visible.
- `parameters: Optional[SomeParameters] = None`
  - Reason: `parameters` already have their own file and dictionary flow. They can also contain launch values that JSON cannot represent cleanly.

### From launch.actions.ExecuteProcess

- `name: Optional[SomeSubstitutionsType] = None`
  - JSON: string preferred. `list[string]` and null accepted.
  - Meaning for `ExecuteProcess`: launch process label.
  - Meaning changes by target action: for `Node`, `name` is the ROS node name.
- `prefix: Optional[SomeSubstitutionsType] = None`
  - JSON: string preferred. `list[string]` and null accepted.
- `cwd: Optional[SomeSubstitutionsType] = None`
  - JSON: string preferred. `list[string]` and null accepted.
- `env: Optional[Dict[SomeSubstitutionsType, SomeSubstitutionsType]] = None`
  - JSON: object with string keys and string values, or null.
- `additional_env: Optional[Dict[SomeSubstitutionsType, SomeSubstitutionsType]] = None`
  - JSON: object with string keys and string values, or null.

Rejected from `launch.actions.ExecuteProcess`:

- `cmd: Iterable[SomeSubstitutionsType]`
  - Reason: `cmd` says which command is executed. A launch file that creates `ExecuteProcess` should pass it explicitly.

### From launch.actions.ExecuteLocal

- `shell: bool = False`
  - JSON: boolean.
- `sigterm_timeout: SomeSubstitutionsType = LaunchConfiguration('sigterm_timeout', default=5)`
  - JSON: string preferred. `list[string]` accepted.
  - Example: `"2.5"` is preferred. `["2", ".", "5"]` is accepted and becomes `"2.5"`.
  - Warning: `list[string]` is concatenated, so `["2", "5"]` becomes `"25"` seconds.
- `sigkill_timeout: SomeSubstitutionsType = LaunchConfiguration('sigkill_timeout', default=5)`
  - JSON: string preferred. `list[string]` accepted.
  - Example: `"10"` is preferred. `["1", "0"]` is accepted and becomes `"10"`.
  - Warning: `list[string]` is concatenated, so it does not represent several timeout values.
- `emulate_tty: bool = False`
  - JSON: boolean.
- `output: SomeSubstitutionsType = 'log'`
  - JSON: string preferred. `list[string]` accepted.
  - Do not document or support internal output dictionary forms. The public `ExecuteLocal` constructor says that `output` is `SomeSubstitutionsType`, so this helper follows that public API.
- `output_format: Text = '[{this.process_description.final_name}] {line}'`
  - JSON: string.
- `cached_output: bool = False`
  - JSON: boolean.
- `log_cmd: bool = False`
  - JSON: boolean.
- `respawn: Union[bool, SomeSubstitutionsType] = False`
  - JSON: boolean.
  - Note: document this field as boolean only. In Jazzy, plain strings such as `"true"` fail when passed directly from Python. For this helper, the useful JSON form is therefore a boolean: `true` or `false`.
- `respawn_delay: Optional[float] = None`
  - JSON: number or null.
- `respawn_max_retries: int = -1`
  - JSON: integer.

Rejected from `launch.actions.ExecuteLocal`:

- `process_description: Executable`
  - Reason: `ExecuteProcess` builds this object internally from `cmd` and process-level fields.
- `on_exit: Optional[Union[SomeEntitiesType, Callable[..., Optional[SomeEntitiesType]]]] = None`
  - Reason: `on_exit` usually contains launch actions or Python callbacks. JSON cannot represent those values cleanly.

Rejected from `launch.action.Action`:

- `condition: Optional[Condition] = None`
  - Reason: conditions are launch expressions. They are clearer when written explicitly in the launch file.

## Future extensions

- A file-based companion such as `bridge_arguments_file` can be added later for long project-level configurations.
- If a future ROS 2 release changes the constructor signatures, update the known, allowed, and rejected field sets together. The tests should keep checking those sets against the target ROS 2 version.
- Python defaults could support richer Python-only values later, but that should be an explicit design change. Today defaults intentionally follow the JSON-compatible value shapes, except that remapping pairs may be written as Python tuples.
- More examples can be added for real launch files when a package needs to expose several actions through one root launch file.
