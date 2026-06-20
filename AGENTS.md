# ros2_launch_helpers Instructions

These instructions apply only to this package.

## Architecture

This package separates reusable launch logic from ROS launch runtime integration.

Use helper functions for rules that can run from explicit Python values. Use launch `Action` classes for behavior that participates in the ROS launch execution graph.

## Helpers

Use helpers when code receives concrete Python values, returns concrete Python values, and can be tested without running a launch description.

Helpers should contain validation, parsing, path resolution, naming rules, and file rendering rules that are reusable outside an `Action`.

## Actions

Use launch `Action` classes when code reads from `LaunchContext`, resolves `LaunchConfiguration`, writes launch configurations, returns launch entities, or must run at a specific point in the launch graph.

Actions should be thin: read runtime values, call helper functions for the actual rule, and write derived launch configuration values directly into the launch context.

## Public API

Do not add public functions whose main purpose is to be passed to `OpaqueFunction`.

Prefer explicit actions such as:

```python
SetRobotNamespace()
ProcessParamsFile()
```

over callback APIs such as:

```python
OpaqueFunction(function=set_robot_namespace)
```

## Naming

Use `compute_*` for pure derived values.

Use `resolve_*` for path, URI, launch config, or layered configuration resolution.

Use `render_*` for functions that write rendered output.

Use imperative class names for actions, such as `SetRobotNamespace` or `ProcessParamsFile`.

## Tests

Test detailed rules on helpers.

Test launch-context integration on actions.

Every action should have tests that prove which launch configuration keys it reads and writes.
