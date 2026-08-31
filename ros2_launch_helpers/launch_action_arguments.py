"""
Resolve action arguments for Node, ExecuteProcess, and ExecuteLocal actions.

The Node, ExecuteProcess, and ExecuteLocal actions have many optional fields. Launch files often
expose only a few of those fields as launch arguments, the ones that are most likely to be
overridden by the user: `remappings`, `output`, `respawn`, `prefix`, or `ros_arguments`.

A few months into a project, the launch files may need to expose more fields, and the number of
launch arguments may grow to a point where it is hard to maintain. If one launch file includes
another launch file, the caller may also need to expose the arguments of the included file.

To avoid that growth, this module provides a JSON-based mechanism to pass several action arguments
through one launch argument. Launch files can also provide Python `default_arguments`; those
defaults are validated by the same policy and are overridden by values from the JSON object.
"""

import json
import math
from typing import Any, Callable, Dict, List, Optional, Tuple, cast

LAUNCH_ACTION_ARGUMENTS_DESC = (
    'JSON string containing supported arguments for one launch_ros.actions.Node, '
    'launch.actions.ExecuteProcess, or launch.actions.ExecuteLocal action.'
)

# Class hierarchy for launch actions:
# Node -> ExecuteProcess -> ExecuteLocal -> Action

# These sets describe constructor arguments that exist in Actions Node, ExecuteProcess, and
# ExecuteLocal.
# They are used to validate user-provided JSON values.
# The sets are derived from the Jazzy ROS 2 launch source code, and they should be updated if the
# launch action constructors change.

_ACTION_KNOWN_ARGUMENTS = {'condition'}

_EXECUTE_LOCAL_DECLARED_ARGUMENTS = {
    'process_description',
    'shell',
    'sigterm_timeout',
    'sigkill_timeout',
    'emulate_tty',
    'output',
    'output_format',
    'cached_output',
    'log_cmd',
    'on_exit',
    'respawn',
    'respawn_delay',
    'respawn_max_retries',
}

_EXECUTE_PROCESS_DECLARED_ARGUMENTS = {'cmd', 'prefix', 'name', 'cwd', 'env', 'additional_env'}

_NODE_DECLARED_ARGUMENTS = {
    'executable',
    'package',
    'name',
    'namespace',
    'exec_name',
    'parameters',
    'remappings',
    'ros_arguments',
    'arguments',
}

# ExecuteProcess inherits from ExecuteLocal, and Node inherits from ExecuteProcess.
# The KNOWN sets follow that inheritance chain.
_EXECUTE_LOCAL_KNOWN_ARGUMENTS = _ACTION_KNOWN_ARGUMENTS | _EXECUTE_LOCAL_DECLARED_ARGUMENTS
_EXECUTE_PROCESS_KNOWN_ARGUMENTS = _EXECUTE_LOCAL_KNOWN_ARGUMENTS | _EXECUTE_PROCESS_DECLARED_ARGUMENTS
_NODE_KNOWN_ARGUMENTS = _EXECUTE_PROCESS_KNOWN_ARGUMENTS | _NODE_DECLARED_ARGUMENTS

# Rejected arguments are not allowed to be set through this helper.
# They are still valid constructor arguments, but this helper does not allow users to pass them
# through resolved action arguments, because they define what is launched.
# The launch file should keep them visible.
_ACTION_REJECTED_ARGUMENTS = {'condition'}
_EXECUTE_LOCAL_REJECTED_ARGUMENTS = _ACTION_REJECTED_ARGUMENTS | {'process_description', 'on_exit'}
_EXECUTE_PROCESS_REJECTED_ARGUMENTS = _EXECUTE_LOCAL_REJECTED_ARGUMENTS | {'cmd'}
_NODE_REJECTED_ARGUMENTS = _EXECUTE_PROCESS_REJECTED_ARGUMENTS | {'executable', 'package', 'parameters'}

# Allowed arguments are the KNOWN arguments minus the REJECTED arguments.
# The REJECTED arguments are not allowed to be set through this helper, because they define what is
# launched, and the launch file should keep them visible.
# The ALLOWED arguments are the ones that can be set through this helper.
_EXECUTE_LOCAL_ALLOWED_ARGUMENTS = _EXECUTE_LOCAL_KNOWN_ARGUMENTS - _EXECUTE_LOCAL_REJECTED_ARGUMENTS
_EXECUTE_PROCESS_ALLOWED_ARGUMENTS = _EXECUTE_PROCESS_KNOWN_ARGUMENTS - _EXECUTE_PROCESS_REJECTED_ARGUMENTS
_NODE_ALLOWED_ARGUMENTS = _NODE_KNOWN_ARGUMENTS - _NODE_REJECTED_ARGUMENTS


def default_launch_action_arguments_json_str() -> str:
    """
    Return the default JSON value for a launch action arguments launch argument.

    The default is the empty JSON object, '{}'. Passing this value to one of the
    ``resolve_*_arguments`` functions means that no action-specific arguments are overridden by the
    user.
    """
    return '{}'


def resolve_execute_local_arguments(
    json_str_arguments: Optional[str],
    default_arguments: Optional[Dict[str, Any]] = None,
    extra_rejected_arguments: Optional[set[str]] = None,
) -> Dict[str, Any]:
    """
    Resolve JSON and default arguments for an `ExecuteLocal` action.

    Only `ExecuteLocal` arguments and `Action` arguments are known here.
    Arguments that belong to `ExecuteProcess` or `Node` are rejected.
    When a field appears in both `default_arguments` and the JSON object, the JSON value wins.
    """
    return _resolve_action_arguments(
        json_str_arguments,
        default_arguments=default_arguments,
        known_arguments=_EXECUTE_LOCAL_KNOWN_ARGUMENTS,
        rejected_arguments=_EXECUTE_LOCAL_REJECTED_ARGUMENTS,
        extra_rejected_arguments=extra_rejected_arguments,
        argument_resolver=_resolve_execute_local_argument,
    )


def resolve_execute_process_arguments(
    json_str_arguments: Optional[str],
    default_arguments: Optional[Dict[str, Any]] = None,
    extra_rejected_arguments: Optional[set[str]] = None,
) -> Dict[str, Any]:
    """
    Resolve JSON and default arguments for an `ExecuteProcess` action.

    `ExecuteProcess` inherits from `ExecuteLocal`, so this function accepts allowed arguments
    from both classes.
    Arguments that belong to `Node` are rejected as unsupported.
    When a field appears in both `default_arguments` and the JSON object, the JSON value wins.
    """
    return _resolve_action_arguments(
        json_str_arguments,
        default_arguments=default_arguments,
        known_arguments=_EXECUTE_PROCESS_KNOWN_ARGUMENTS,
        rejected_arguments=_EXECUTE_PROCESS_REJECTED_ARGUMENTS,
        extra_rejected_arguments=extra_rejected_arguments,
        argument_resolver=_resolve_execute_process_argument,
    )


def resolve_node_arguments(
    json_str_arguments: Optional[str],
    default_arguments: Optional[Dict[str, Any]] = None,
    extra_rejected_arguments: Optional[set[str]] = None,
) -> Dict[str, Any]:
    """
    Resolve JSON and default arguments for a `Node` action.

    `Node` inherits from `ExecuteProcess`, which inherits from `ExecuteLocal`.
    This function accepts allowed arguments from all three classes, Node, ExecuteProcess, and
    ExecuteLocal.
    When a field appears in both `default_arguments` and the JSON object, the JSON value wins.
    """
    return _resolve_action_arguments(
        json_str_arguments,
        default_arguments=default_arguments,
        known_arguments=_NODE_KNOWN_ARGUMENTS,
        rejected_arguments=_NODE_REJECTED_ARGUMENTS,
        extra_rejected_arguments=extra_rejected_arguments,
        argument_resolver=_resolve_node_argument,
    )


def resolve_remappings(argument_name: str, argument_value: object) -> Optional[List[Tuple[str, str]]]:
    """
    Convert remappings into the format expected by ROS 2 launch actions.

    A ROS 2 launch action expects each remapping as a tuple: `("from", "to")`.
    When remappings come from JSON, each pair must be written as a two-item list because JSON cannot
    contain tuples: `["from", "to"]`.
    When remappings come from Python defaults, each pair may be written either as a two-item list or
    as a two-item tuple: `["from", "to"]` or `("from", "to")`.

    For example, this JSON value:

    .. code-block:: json

        [["~/reference", "cmd_vel"], ["~/odometry", "odom"]]

    is returned as:

    .. code-block:: python

        [("~/reference", "cmd_vel"), ("~/odometry", "odom")]

    The function also checks that every remapping has exactly two non-empty string values.
    It always returns tuple pairs, regardless of whether the input pair was a list or a tuple.
    """
    if argument_value is None:
        return None

    if not isinstance(argument_value, list):
        raise ValueError(f"launch action argument '{argument_name}' must be a JSON list")

    remappings: List[Tuple[str, str]] = []

    for index, remapping in enumerate(argument_value):
        if not isinstance(remapping, (list, tuple)) or len(remapping) != 2:
            raise ValueError(f"launch action argument '{argument_name}' item {index} must be a two-item list or tuple")

        original_name, new_name = remapping

        if not isinstance(original_name, str) or not original_name:
            raise ValueError(
                f"launch action argument '{argument_name}' item {index} must have a non-empty string source"
            )

        if not isinstance(new_name, str) or not new_name:
            raise ValueError(
                f"launch action argument '{argument_name}' item {index} must have a non-empty string target"
            )

        remappings.append((original_name, new_name))

    return remappings


def _resolve_action_arguments(
    json_str_arguments: Optional[str],
    *,
    default_arguments: Optional[Dict[str, Any]],
    known_arguments: set[str],
    rejected_arguments: set[str],
    extra_rejected_arguments: Optional[set[str]],
    argument_resolver: Callable[[str, object], object],
) -> Dict[str, Any]:
    """
    Resolve defaults and one JSON launch argument into action keyword arguments.

    The public resolver decides which argument names are known and rejected.
    This function enforces that policy, then delegates each value to `argument_resolver` to convert
    it into the Python type expected by ROS 2 launch.
    """
    if default_arguments is None:
        default_arguments = {}
    elif not isinstance(default_arguments, dict):
        raise ValueError('default_arguments must be a dictionary')

    # Validate that the rejected arguments are known by the selected launch action resolver.
    unknown_rejected_arguments = rejected_arguments - known_arguments

    if unknown_rejected_arguments:
        raise ValueError(
            f'internal rejected launch action arguments are not known: {sorted(unknown_rejected_arguments)}'
        )

    # Additional fields can be rejected by the caller, if required for any reason.
    if extra_rejected_arguments is None:
        extra_rejected_arguments = set()
    elif not isinstance(extra_rejected_arguments, set):
        raise ValueError('extra_rejected_arguments must be a set')

    # Extra rejected arguments are caller input.
    # Validate that they are non-empty strings, because the caller may have a typo or a wrong
    # resolver.
    for argument_name in extra_rejected_arguments:
        if not isinstance(argument_name, str) or not argument_name:
            raise ValueError('extra_rejected_arguments must contain non-empty string argument names')

    # A caller can only reject arguments that are known by the selected launch action resolver.
    # Rejecting an unknown name usually means a typo or a wrong resolver.
    unknown_extra_rejected_arguments = extra_rejected_arguments - known_arguments

    if unknown_extra_rejected_arguments:
        raise ValueError(f'rejected launch action arguments are not known: {sorted(unknown_extra_rejected_arguments)}')

    # total_rejected_arguments is the complete policy used to validate defaults and JSON values.
    total_rejected_arguments = rejected_arguments | extra_rejected_arguments

    resolved_default_arguments = _resolve_argument_dict(
        default_arguments,
        known_arguments=known_arguments,
        rejected_arguments=total_rejected_arguments,
        argument_resolver=argument_resolver,
        # Defaults are Python objects owned by the launch file.
        # Copy resolved mutable values so mutating the returned action arguments cannot mutate the
        # launch-file defaults.
        copy_resolved_mutable_values=True,
    )

    # Missing or empty JSON means "use only the launch file defaults".
    if json_str_arguments is None:
        return resolved_default_arguments

    if not isinstance(json_str_arguments, str):
        raise ValueError('json_str_arguments must be a JSON string')

    json_str_arguments = json_str_arguments.strip()

    if not json_str_arguments:
        return resolved_default_arguments

    # Convert the JSON text into ordinary Python values before validating individual fields.
    try:
        arguments = json.loads(json_str_arguments)
    except json.JSONDecodeError as error:
        raise ValueError('json_str_arguments must be valid JSON') from error

    if not isinstance(arguments, dict):
        raise ValueError('json_str_arguments must be a JSON object')

    resolved_arguments = _resolve_argument_dict(
        arguments,
        known_arguments=known_arguments,
        rejected_arguments=total_rejected_arguments,
        argument_resolver=argument_resolver,
        # json.loads creates fresh Python objects for JSON arrays and objects.
        # The resolved result can own those objects directly, so no extra defensive copy is needed
        # for JSON-provided values.
        copy_resolved_mutable_values=False,
    )

    # User JSON overrides launch-file defaults for the same field.
    return {**resolved_default_arguments, **resolved_arguments}


def _resolve_argument_dict(
    arguments: Dict[str, Any],
    *,
    known_arguments: set[str],
    rejected_arguments: set[str],
    argument_resolver: Callable[[str, object], object],
    copy_resolved_mutable_values: bool,
) -> Dict[str, Any]:
    """
    Validate and resolve every field in an argument dictionary.

    Both default arguments and JSON values pass through this function.
    This keeps the security policy in one place.
    It also prevents defaults from bypassing rejected fields.
    `copy_resolved_mutable_values` exists because default arguments and JSON values have different
    ownership.
    Default arguments are Python objects that the launch file may keep and reuse, so their
    mutable values must be copied before they are returned.
    JSON values come from `json.loads`, which already creates fresh Python objects for that resolver
    call.
    """
    resolved_arguments: Dict[str, Any] = {}

    for argument_name, argument_value in arguments.items():
        # JSON object keys should always be strings, but Python defaults could contain any key.
        if not isinstance(argument_name, str) or not argument_name:
            raise ValueError('launch action argument name must be a non-empty string')

        # Unknown means "this action class does not have such an argument".
        if argument_name not in known_arguments:
            raise ValueError(f"launch action argument '{argument_name}' is not supported")

        # Rejected means "this argument exists, but this helper intentionally keeps it in code".
        if argument_name in rejected_arguments:
            raise ValueError(
                f"launch action argument '{argument_name}' is not allowed; set it directly in the launch file"
            )

        resolved_argument_value = argument_resolver(argument_name, argument_value)

        if copy_resolved_mutable_values:
            resolved_argument_value = _copy_resolved_mutable_value(resolved_argument_value)

        resolved_arguments[argument_name] = resolved_argument_value

    return resolved_arguments


def _copy_resolved_mutable_value(argument_value: object) -> object:
    """
    Return a shallow copy when the caller asks to detach a resolved mutable value.

    Resolvers only produce simple launch-compatible values. A shallow copy is enough for the mutable
    containers supported by this module: ``dict[str, str]`` and ``list[str]``.
    Remappings are already rebuilt as a new list of tuple pairs by ``resolve_remappings``.
    """
    if isinstance(argument_value, dict):
        return dict(argument_value)

    if isinstance(argument_value, list):
        return list(argument_value)

    return argument_value


def _resolve_execute_local_argument(argument_name: str, argument_value: object) -> object:
    """
    Resolve one allowed argument declared by ``ExecuteLocal``.
    """
    match argument_name:
        case 'shell' | 'emulate_tty' | 'cached_output' | 'log_cmd' | 'respawn':
            return _resolve_argument_bool(argument_name, argument_value)
        case 'sigterm_timeout' | 'sigkill_timeout' | 'output':
            return _resolve_argument_substitution(argument_name, argument_value)
        case 'output_format':
            return _resolve_argument_str(argument_name, argument_value)
        case 'respawn_delay':
            return _resolve_argument_optional_float(argument_name, argument_value)
        case 'respawn_max_retries':
            return _resolve_argument_int(argument_name, argument_value)
        case _:
            raise ValueError(f"launch action argument '{argument_name}' is not supported")


def _resolve_execute_process_argument(argument_name: str, argument_value: object) -> object:
    """
    Resolve one allowed argument declared by ``ExecuteProcess`` or inherited from ``ExecuteLocal``.
    """
    match argument_name:
        case 'name' | 'prefix' | 'cwd':
            return _resolve_argument_optional_substitution(argument_name, argument_value)
        case 'env' | 'additional_env':
            return _resolve_argument_optional_string_dict(argument_name, argument_value)
        case _:
            return _resolve_execute_local_argument(argument_name, argument_value)


def _resolve_node_argument(argument_name: str, argument_value: object) -> object:
    """
    Resolve one allowed argument declared by ``Node`` or inherited from its base classes.
    """
    match argument_name:
        case 'namespace' | 'exec_name':
            return _resolve_argument_optional_substitution(argument_name, argument_value)
        case 'ros_arguments' | 'arguments':
            return _resolve_argument_optional_string_list(argument_name, argument_value)
        case 'remappings':
            return resolve_remappings(argument_name, argument_value)
        case _:
            return _resolve_execute_process_argument(argument_name, argument_value)


def _resolve_argument_bool(argument_name: str, argument_value: object) -> bool:
    """
    Return one launch action argument as a Python boolean.

    JSON boolean values become ``True`` or ``False`` after ``json.loads``. Python defaults must also
    use real booleans. Strings such as ``"true"`` are rejected because ROS 2 launch expects a real
    boolean for these fields.
    """
    if not isinstance(argument_value, bool):
        raise ValueError(f"launch action argument '{argument_name}' must be a boolean")

    return argument_value


def _resolve_argument_int(argument_name: str, argument_value: object) -> int:
    """
    Return one launch action argument as a Python integer.

    This is used for fields such as ``respawn_max_retries``. Booleans are rejected even though
    Python treats ``bool`` as a subclass of ``int``.
    """
    if isinstance(argument_value, bool) or not isinstance(argument_value, int):
        raise ValueError(f"launch action argument '{argument_name}' must be an integer")

    return argument_value


def _resolve_argument_string_list(
    argument_name: str, argument_value: object, expected_type: str = 'list of strings'
) -> List[str]:
    """
    Return one launch action argument as a list of strings.

    The value may come from a JSON array or from a Python default. The value itself must be a list,
    and every item in it must be a string. ``expected_type`` is only used to make the error message
    match the field being resolved.
    """
    if not isinstance(argument_value, list):
        raise ValueError(f"launch action argument '{argument_name}' must be a {expected_type}")

    for index, item in enumerate(argument_value):
        if not isinstance(item, str):
            raise ValueError(f"launch action argument '{argument_name}' item {index} must be a string")

    return cast(List[str], argument_value)


def _resolve_argument_optional_string_dict(argument_name: str, argument_value: object) -> Optional[Dict[str, str]]:
    """
    Return one launch action argument as ``None`` or ``dict[str, str]``.

    This is used for fields such as ``env`` and ``additional_env``. ``None`` is accepted because
    those launch action fields are optional. When a dictionary is provided, every key must be a
    non-empty string and every value must be a string.
    """
    if argument_value is None:
        return None

    if not isinstance(argument_value, dict):
        raise ValueError(f"launch action argument '{argument_name}' must be a JSON object")

    for env_key, env_value in argument_value.items():
        if not isinstance(env_key, str) or not env_key:
            raise ValueError(f"launch action argument '{argument_name}' must have non-empty string keys")

        if not isinstance(env_value, str):
            raise ValueError(f"launch action argument '{argument_name}.{env_key}' must be a string")

    return cast(Dict[str, str], argument_value)


def _resolve_argument_optional_float(argument_name: str, argument_value: object) -> Optional[float]:
    """
    Return one launch action argument as ``None`` or a finite Python float.

    This is used for fields such as ``respawn_delay``. Integers are accepted and converted to floats
    because that does not change the meaning of the value. Booleans are rejected even though Python
    treats ``bool`` as a subclass of ``int``.
    """
    if argument_value is None:
        return None

    if isinstance(argument_value, bool) or not isinstance(argument_value, (float, int)):
        raise ValueError(f"launch action argument '{argument_name}' must be a number or null")

    if not math.isfinite(float(argument_value)):
        raise ValueError(f"launch action argument '{argument_name}' must be finite")

    return float(argument_value)


def _resolve_argument_optional_string_list(argument_name: str, argument_value: object) -> Optional[List[str]]:
    """
    Return one launch action argument as ``None`` or ``list[str]``.

    This is used for fields such as ``ros_arguments`` and ``arguments``. ``None`` is accepted
    because those launch action fields are optional. Non-``None`` values are resolved by
    ``_resolve_argument_string_list``.
    """
    if argument_value is None:
        return None

    return _resolve_argument_string_list(argument_name, argument_value)


def _resolve_argument_optional_substitution(argument_name: str, argument_value: object) -> Optional[str | List[str]]:
    """
    Return one optional launch substitution argument.

    This is used for optional ROS 2 ``SomeSubstitutionsType`` fields. ``None`` is accepted because
    those launch action fields are optional. Non-``None`` values are resolved by
    ``_resolve_argument_substitution``.
    """
    if argument_value is None:
        return None

    return _resolve_argument_substitution(argument_name, argument_value)


def _resolve_argument_str(argument_name: str, argument_value: object) -> str:
    """
    Return one launch action argument as a string.

    This is used for fields such as ``output_format``. ``None`` and non-string values are rejected
    because the corresponding launch action field requires a string.
    """
    if not isinstance(argument_value, str):
        raise ValueError(f"launch action argument '{argument_name}' must be a string")

    return argument_value


def _resolve_argument_substitution(argument_name: str, argument_value: object) -> str | List[str]:
    """
    Return one required launch substitution argument.

    Some ROS 2 launch action fields use ``SomeSubstitutionsType``. In ROS 2 Jazzy, that type is
    defined in:

    ``/opt/ros/jazzy/lib/python3.12/site-packages/launch/some_substitutions_type.py``

    The definition is:

    .. code-block:: python

        SomeSubstitutionsType = Union[
            Text,
            Path,
            Substitution,
            Iterable[Union[Text, Path, Substitution]],
        ]

    Values coming from ``json.loads`` cannot be Python ``Path`` objects or ROS 2 launch
    ``Substitution`` objects, such as ``LaunchConfiguration`` or ``FindPackageShare``. For
    consistency with the JSON path, Python defaults also support only the JSON-compatible part of
    ``SomeSubstitutionsType``: one string or a list of strings.
    """
    if isinstance(argument_value, str):
        return argument_value

    return _resolve_argument_string_list(argument_name, argument_value, 'string or list of strings')
