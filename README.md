# ros2_launch_helpers

`ros2_launch_helpers` standardizes common ROS 2 launch-file configuration.

## Purpose

This package provides functions and utilities to:

- Declare standard launch arguments for parameter files, overlays, namespaces, node options, node remappings, and node logging options.
- Use explicit launch actions for common context updates such as robot namespaces, robot prefixes, and rendered parameter files.
- Build parameter layers from a base file and additional overlays.
- Parse node options, node remappings, and node logging options from JSON strings.
- Convert the parsed values into the structures expected by `launch_ros.actions.Node`.

## Launch Actions

Use actions when a launch file needs to read values from the launch context and write derived values back into the launch context. The actions are thin wrappers around helper functions, so the naming and rendering rules remain testable without running a full launch description.

```python
import ros2_launch_helpers as rlh
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration

DeclareLaunchArgument('namespace', default_value='')
DeclareLaunchArgument('robot_name', default_value='robot_1')
DeclareLaunchArgument('params_file')
DeclareLaunchArgument('params_file_allow_substs', default_value='true')

rlh.SetGlobalNamespace(namespace=LaunchConfiguration('namespace'), output_namespace_key='namespace')
rlh.SetRobotNamespace(
    namespace=LaunchConfiguration('namespace'),
    robot_name=LaunchConfiguration('robot_name'),
    robot_namespace_key='robot_namespace',
)
rlh.SetRobotPrefix(robot_name=LaunchConfiguration('robot_name'), robot_prefix_key='robot_prefix')
rlh.ProcessParamsFile(
    params_file=LaunchConfiguration('params_file'),
    allow_substs=LaunchConfiguration('params_file_allow_substs'),
    output_params_file_key='params_file',
)
```

Action inputs that read launch context values accept either a launch configuration name string or a `LaunchConfiguration` object. Passing `LaunchConfiguration(...)` makes the runtime lookup explicit.

- `SetGlobalNamespace()` reads `namespace` and writes the absolute namespace back to `namespace`.
- `SetRobotNamespace()` reads `namespace` and `robot_name`, then writes `robot_namespace`.
- `SetRobotPrefix()` reads `robot_name`, then writes `robot_prefix`.
- `ProcessParamsFile()` reads a resolved filesystem path from `params_file` and reads `params_file_allow_substs`, then writes the same path or the rendered path back to `params_file`.

The lower-level helpers remain available for code that already has concrete values:

```python
robot_namespace = rlh.compute_robot_namespace('/robots', 'front')
robot_prefix = rlh.compute_robot_prefix('front')
```

## Example Usage

Each launch file resolves the effective node name from its own launch argument. The node name is not read from the JSON options. This keeps node identity explicit in the launch file that creates the node, while the JSON strings keep the node options, remappings, and logging options easy to pass through `IncludeLaunchDescription`.

```python
import ros2_launch_helpers as rlh
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration

DeclareLaunchArgument(
    'node_name',
    default_value='ground_vehicle_twist_odometry',
    description='Node name',
)
DeclareLaunchArgument(
    'node_options',
    default_value=rlh.default_node_options_json_str(),
    description=rlh.NODE_OPTIONS_DESC,
)
DeclareLaunchArgument(
    'node_logging_options',
    default_value=rlh.default_node_logging_options_json_str(),
    description=rlh.LOGGING_OPTIONS_DESC,
)
DeclareLaunchArgument(
    'node_remappings',
    default_value=rlh.default_node_remappings_json_str(),
    description=rlh.REMAPPINGS_DESC,
)

node_name = LaunchConfiguration('node_name').perform(ctx)
node_options_by_name, remappings_by_name, ros_arguments_by_name = (
    rlh.resolve_node_launch_configs(
        node_names=[node_name],
        node_options=LaunchConfiguration('node_options').perform(ctx),
        node_logging_options=LaunchConfiguration('node_logging_options').perform(ctx),
        node_remappings=LaunchConfiguration('node_remappings').perform(ctx),
    )
)

node_options = node_options_by_name[node_name]
remappings = remappings_by_name[node_name]
ros_arguments = ros_arguments_by_name[node_name]

parameters = rlh.get_parameters(params_file, overlay_params_file_list)
```

For launch files that create more than one node, pass all effective node names in `node_names`. The function returns one dictionary per output type, indexed by node name.

## Node JSON Arguments

The node-related launch arguments are JSON objects indexed by effective node name. Their default launch argument value is `"{}"`, which means "there are no overrides for any node". The real defaults for node options and logging are stored in `ros2_launch_helpers` and are applied when a node has no entry or only a partial entry.

`node_options` is indexed by the effective node name. It does not contain a `name` field:

```json
{
  "front_odometry": {
    "output": "screen",
    "emulate_tty": true,
    "respawn": false,
    "respawn_delay": 0.0
  }
}
```

`node_logging_options` is indexed by the effective node name. Known keys configure the node logger. Any other key is treated as a custom logger name:

```json
{
  "front_odometry": {
    "log-level": "info",
    "disable-stdout-logs": false,
    "disable-rosout-logs": false,
    "disable-external-lib-logs": false,
    "some.logger.name": "debug"
  }
}
```

Use `log-level` when you want to configure the main logger for the node launched by this launch file:

```json
{
  "front_odometry": {
    "log-level": "debug"
  }
}
```

Use an explicit logger name when the code creates or uses a logger with that exact name. For example, code that calls `rclcpp::get_logger("solver")` is configured with the `solver` key:

```json
{
  "front_odometry": {
    "solver": "debug"
  }
}
```

Use the full child logger name when the code creates a child logger from the node logger. For a node named `front_odometry`, code that calls `node->get_logger().get_child("twist_cb")` usually creates the logger `front_odometry.twist_cb`:

```json
{
  "front_odometry": {
    "front_odometry.twist_cb": "debug"
  }
}
```

If the node is launched in the `robot` namespace, the logger name can include that namespace. In that case, configure the full logger name:

```json
{
  "front_odometry": {
    "robot.front_odometry.twist_cb": "debug"
  }
}
```

The `front_odometry` key outside the logging options selects which launched node receives these ROS arguments. The keys inside that object are either known logging options or exact logger names passed to ROS 2.

`node_remappings` is indexed by the effective node name:

```json
{
  "front_odometry": [
    "/input_twist:=/cmd_vel",
    "odom:=wheel_odom"
  ]
}
```

The effective node name is the value passed to the launch file through its local `node_name` argument, or the launch file default when no parent launch file overrides it. If a parent launch file changes `node_name`, the JSON entries for node options, logging options, and remappings must use the new effective node name.

The JSON objects may contain entries for nodes that a launch file does not use. Those entries are ignored by that launch file and are not validated unless that node name is requested.

## When to Use

- When a launch file should accept structured node options without declaring one launch argument per option.
- When parent launch files should be able to pass node remappings or logging options to included launch files.
- When a launch file creates one or more nodes and needs the processed options in the exact shape expected by `launch_ros.actions.Node`.
