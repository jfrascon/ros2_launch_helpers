# ros2_launch_helpers instructions

These instructions apply only to this package.

## Architecture

This package separates reusable launch logic from ROS 2 launch runtime integration.

Use helper functions for rules that can run from explicit Python values. Use launch `Action` classes for behavior that participates in the ROS 2 launch execution graph.

Keep actions thin. An action should read runtime values from the launch context, call helper functions for the actual rule, and write the derived value back into the launch context.

Do not keep old compatibility wrappers after an API has intentionally been replaced. If the package changes API, update the callers and tests instead of leaving two supported ways to do the same thing.

## Helpers

Use helpers when code receives concrete Python values, returns concrete Python values, and can be tested without running a launch description.

Helpers should contain validation, parsing, path resolution, naming rules, YAML loading, parameter layering, and file rendering rules that are reusable outside an `Action`.

Validate public helper inputs defensively when invalid values would produce unclear errors or make future callers easy to misuse. Private helpers may rely on nearby validation when the invariant is clear.

Use these naming patterns:

- `compute_*` for pure derived values.
- `resolve_*` for path, URI, launch configuration, JSON, or layered configuration resolution.
- `render_*` for functions that write rendered output.
- `to_*` for simple conversion helpers.

## Actions

Use launch `Action` classes when code reads from `LaunchContext`, resolves `LaunchConfiguration`, writes launch configurations, returns launch entities, or must run at a specific point in the launch graph.

Action constructor inputs that read launch context values should accept only two forms:

- a string key, such as `'robot_name'`
- a `LaunchConfiguration` object, such as `LaunchConfiguration('robot_name')`

Do not accept generic substitutions for those inputs unless there is a concrete reason. The goal is to keep the launch configuration key visible at the call site.

Actions should write simple derived launch configuration values directly into `context.launch_configurations`. Do not return `SetLaunchConfiguration` actions for that case.

Pass `**kwargs` to the base `Action` class so standard launch features such as `condition` remain available. Do not add explicit constructor parameters such as `condition` unless the action needs package-specific behavior.

Use imperative class names for actions, such as `SetRobotNamespace` or `ProcessParamsFile`.

## Launch action options

Use `launch_action_options_json_str` for optional JSON-configurable fields from `Node`, `ExecuteProcess`, and `ExecuteLocal`.

The helper should be strict. It should reject unknown fields, reject fields that belong explicitly in the launch file, and validate the JSON type for every supported field.

Do not make this helper a second launch language. Fields such as `package`, `executable`, `namespace`, `parameters`, and `cmd` should stay visible in Python launch code.

If a supported ROS 2 field is `Optional[...]`, JSON `null` should be accepted and returned as Python `None`. If a supported ROS 2 field is not optional, JSON `null` should be rejected.

When the supported field list changes, update all three places together:

- `ros2_launch_helpers/helpers.py`
- `tests/test_launch_action_options.py`
- `doc/launch_action_options_design.md`

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

## Documentation

Write package documentation in a plain, pedagogical style. Prefer direct sentences that explain one idea at a time.

Use sentence-case headings, for example `## What the helper does and does not do`.

Keep README examples small and runnable in spirit. If an example uses a launch context, show where that context comes from, usually an `OpaqueFunction` callback.

Use the README for the short usage path. Use `doc/launch_action_options_design.md` for longer design reasoning and detailed field references.

## Tests

Test detailed rules on helpers.

Test launch-context integration on actions.

Every action should have tests that prove which launch configuration keys it reads and writes.

For `launch_action_options_json_str`, test valid values, invalid field names, rejected fields, invalid JSON types, `null` handling for optional fields, and rejection of `null` for non-optional fields.
