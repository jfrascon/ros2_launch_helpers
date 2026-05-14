# ros2_launch_helpers

`ros2_launch_helpers` standardizes common ROS 2 launch-file configuration.

## Purpose

This package provides functions and utilities to:

- Declare standard launch arguments for parameter files, overlays, namespaces, node options,
  node remappings, and node logging options.
- Build parameter layers from a base file and additional overlays.
- Parse node options, node remappings, and node logging options from JSON strings.
- Convert the parsed values into the structures expected by `launch_ros.actions.Node`.

## Example Usage

Each launch file resolves the effective node name from its own launch argument. The node name is
not read from the JSON options. This keeps node identity explicit in the launch file that creates
the node, while the JSON strings keep the node options, remappings, and logging options easy to
pass through `IncludeLaunchDescription`.

```python
import ros2_launch_helpers as rlh

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

For launch files that create more than one node, pass all effective node names in `node_names`.
The function returns one dictionary per output type, indexed by node name.

## Node JSON Arguments

The node-related launch arguments are JSON objects indexed by effective node name. Their default
launch argument value is `"{}"`, which means "there are no overrides for any node". The real
defaults for node options and logging are stored in `ros2_launch_helpers` and are applied when a
node has no entry or only a partial entry.

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

`node_logging_options` is indexed by the effective node name. Known keys configure the node
logger. Any other key is treated as a custom logger name:

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

Use `log-level` when you want to configure the main logger for the node launched by this launch
file:

```json
{
  "front_odometry": {
    "log-level": "debug"
  }
}
```

Use an explicit logger name when the code creates or uses a logger with that exact name. For
example, code that calls `rclcpp::get_logger("solver")` is configured with the `solver` key:

```json
{
  "front_odometry": {
    "solver": "debug"
  }
}
```

Use the full child logger name when the code creates a child logger from the node logger. For a
node named `front_odometry`, code that calls `node->get_logger().get_child("twist_cb")` usually
creates the logger `front_odometry.twist_cb`:

```json
{
  "front_odometry": {
    "front_odometry.twist_cb": "debug"
  }
}
```

If the node is launched in the `robot` namespace, the logger name can include that namespace. In
that case, configure the full logger name:

```json
{
  "front_odometry": {
    "robot.front_odometry.twist_cb": "debug"
  }
}
```

The `front_odometry` key outside the logging options selects which launched node receives these
ROS arguments. The keys inside that object are either known logging options or exact logger names
passed to ROS 2.

`node_remappings` is indexed by the effective node name:

```json
{
  "front_odometry": [
    "/input_twist:=/cmd_vel",
    "odom:=wheel_odom"
  ]
}
```

The effective node name is the value passed to the launch file through its local `node_name`
argument, or the launch file default when no parent launch file overrides it. If a parent launch
file changes `node_name`, the JSON entries for node options, logging options, and remappings must
use the new effective node name.

The JSON objects may contain entries for nodes that a launch file does not use. Those entries are
ignored by that launch file and are not validated unless that node name is requested.

## When to Use

- When a launch file should accept structured node options without declaring one launch argument
  per option.
- When parent launch files should be able to pass node remappings or logging options to included
  launch files.
- When a launch file creates one or more nodes and needs the processed options in the exact shape
  expected by `launch_ros.actions.Node`.
