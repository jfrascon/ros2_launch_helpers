
# ros2_launch_helpers

`ros2_launch_helpers` is a helper library designed to standardize and simplify argument, parameter, and option management in ROS 2 launch files.

## Purpose

This package provides functions and utilities to:
- Declare standard arguments in launch files, such as parameter files, overlays, namespace, remappings, node options, and ROS arguments.
- Build parameter layers from a base file and additional overlays, enabling flexible and hierarchical configuration.
- Manage remappings, node options, and ROS arguments from the command line, YAML files, or launch arguments, with clear and predictable precedence.
- Easily apply these parameters and options to Nodes or LifecycleNodes in your launch files, simplifying integration and reuse.

## Example Usage

In a typical launch file, you can import and use the helpers like this:

```python
import ros2_launch_helpers as rlh

DeclareLaunchArgument('remappings', default_value='', description=rlh.REMAPPINGS_DESC)
DeclareLaunchArgument('log_options', default_value=rlh.default_log_options_str(), description=rlh.LOG_OPTIONS_DESC)
DeclareLaunchArgument('node_options', default_value=rlh.default_node_options_str(), description=rlh.NODE_OPTIONS_DESC)

parameters = rlh.get_params(params_file, overlay_params_file_list)
```

This allows your nodes to receive advanced and customizable configurations in a simple and consistent way.

## When to Use

- When you want your launch files to be easily configurable and reusable.
- If you need to support parameter overlays, remappings, and advanced node options without duplicating logic.
- To maintain a clear and standard launch interface across your ROS 2 projects.
