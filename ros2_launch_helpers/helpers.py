import json
import math
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import yaml
from ament_index_python.packages import get_package_share_directory
from launch import LaunchContext, LaunchDescriptionEntity
from launch.actions import LogInfo
from launch_ros.parameter_descriptions import ParameterFile

LAUNCH_ACTION_ARGUMENTS_DESC = (
    'JSON string containing supported arguments for one launch_ros.actions.Node, '
    'launch.actions.ExecuteProcess, or launch.actions.ExecuteLocal action.'
)

_ALL_LAUNCH_ACTION_ARGUMENTS = {
    # From Node.
    'executable',
    'package',
    'name',
    'namespace',
    'exec_name',
    'parameters',
    'remappings',
    'ros_arguments',
    'arguments',
    # From ExecuteProcess.
    'cmd',
    'prefix',
    'cwd',
    'env',
    'additional_env',
    # From ExecuteLocal.
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
    # From Action.
    'condition',
}

_REJECTED_LAUNCH_ACTION_ARGUMENTS = {
    # From Node.
    'executable',
    'package',
    'parameters',
    # From ExecuteProcess.
    'cmd',
    # From ExecuteLocal.
    'process_description',
    'on_exit',
    # From Action.
    'condition',
}


class FileResolutionError(ValueError):
    """
    Base exception raised when this module cannot convert a user-provided file value into a
    filesystem path.

    Callers can catch this class when they do not need to distinguish between an empty value, an
    invalid URI shape, or another file-resolution error reported by this module.
    """


class NullFilePathError(FileResolutionError):
    """
    Raised when a function needs a file path or URI, but the caller passed ``None`` or an empty
    string.
    """


class InvalidFileUriPatternError(FileResolutionError):
    """
    Raised when a URI-like file value is not one of the formats accepted by ``resolve_file``.

    This includes malformed ``package://`` values, malformed ``file://`` values, and unsupported
    URI schemes such as ``http://``.
    """


def compute_global_namespace(namespace: str) -> str:
    """
    Return the global namespace that should be stored in the launch context.

    ``namespace`` is the namespace value provided by the user. It can be empty, relative, absolute,
    or ``/``. The returned value is resolved from the root namespace, so a relative value such as
    ``robots`` becomes ``/robots``.

    This function only applies the naming rule. The launch action that calls it is responsible for
    reading ``namespace`` from the launch context and writing the resolved value back.
    """
    return resolve_name('/', namespace)


def compute_robot_namespace(namespace: str, robot_name: str) -> str:
    """
    Return the namespace for one robot inside a parent namespace.

    ``namespace`` is the parent namespace and ``robot_name`` is the child name that identifies the
    robot. If ``namespace`` is ``/fleet`` and ``robot_name`` is ``mima``, the returned namespace is
    ``/fleet/mima``.

    This function only applies the naming rule. It does not read or write launch configurations.
    """
    return resolve_name(namespace, robot_name)


def compute_robot_prefix(robot_name: str) -> str:
    """
    Return the prefix derived from one robot name.

    The prefix is used by callers that need a stable text prefix for frame names, topic names, or
    other generated identifiers. If ``robot_name`` already ends with ``_``, it is returned as-is.
    Otherwise, one trailing ``_`` is added.
    """
    return to_prefix(robot_name)


def default_launch_action_arguments_json_str() -> str:
    """
    Return the default JSON value for a launch action arguments launch argument.

    The default is the empty JSON object, ``{}``. Passing this value to
    ``resolve_launch_action_arguments`` means that no action-specific arguments are overridden by
    the user.
    """
    return '{}'


def resolve_remappings(argument_name: str, argument_value: object) -> Optional[List[Tuple[str, str]]]:
    """
    Convert remappings read from JSON into the format expected by ROS 2 launch actions.

    A ROS 2 launch action expects each remapping as a tuple: ``("from", "to")``.
    A JSON string cannot contain Python tuples, so launch arguments must write each
    remapping as a two-item list: ``["from", "to"]``.

    For example, this JSON value:

    .. code-block:: json

        [["~/reference", "cmd_vel"], ["~/odometry", "odom"]]

    is returned as:

    .. code-block:: python

        [("~/reference", "cmd_vel"), ("~/odometry", "odom")]

    The function also checks that every remapping has exactly two non-empty string
    values. Code outside this module should call this public function instead of
    calling the private helper used by ``resolve_launch_action_arguments``.
    """
    return _resolve_argument_remappings(argument_name, argument_value)


def flatten_namespace(namespace: str, new_sep: str) -> str:
    """
    Convert a ROS namespace into one flat string.

    ``namespace`` must be a valid ROS namespace. The leading and trailing ``/`` characters are
    removed before the inner ``/`` separators are replaced with ``new_sep``.

    For example, ``/fleet/robot1`` with ``new_sep="_"`` returns ``fleet_robot1``.
    The root namespace ``/`` and the empty namespace ``''`` both return ``''`` because they do not
    contain a concrete namespace segment.
    """
    if not isinstance(namespace, str):
        raise ValueError('Namespace must be a string')

    if not is_valid_namespace(namespace):
        raise RuntimeError(f"Invalid namespace '{namespace}'")

    if namespace in ('', '/'):
        return ''

    return replace_separator_in_namespace(namespace.strip('/'), new_sep)


def get_parameters(params_file: str, overlay_params_file_list: str = '') -> list[Any]:
    """
    Build the ``parameters`` list for a ROS 2 launch ``Node``.

    ``params_file`` is the required base YAML file. ``overlay_params_file_list`` is an optional
    comma-separated list of extra YAML files. Empty overlay entries, duplicate overlay entries, and
    an overlay equal to ``params_file`` are ignored.

    Each returned item is a ``ParameterFile`` with substitutions enabled. The returned list can be
    passed directly to the ``parameters`` field of a ``Node``.
    """
    params_file = params_file.strip()

    if not params_file:
        raise ValueError('params_file is required')

    parameters = [ParameterFile(params_file, allow_substs=True)]

    if not overlay_params_file_list:
        return parameters

    seen: set[str] = set()

    for p_file in overlay_params_file_list.split(','):
        p_file = p_file.strip()

        if not p_file or p_file == params_file or p_file in seen:
            continue

        seen.add(p_file)

        # Defer substitutions, env, package shares to launch.
        parameters.append(ParameterFile(p_file, allow_substs=True))

    return parameters


def is_valid_name(s: str) -> bool:
    """
    Return whether one name segment is valid for this helper module.

    A valid segment is a non-empty string with at most 255 characters. It must not start with a
    number, and every character must be ASCII alphanumeric or ``_``.

    The rule intentionally matches the ROS 2 node-name validation style used by this package. The
    function returns ``False`` instead of raising because callers often use it inside validation
    paths that build their own error message.
    """
    if not isinstance(s, str):
        return False

    if not s:
        return False

    # ROS 2 node name max length.
    if len(s) > 255:
        return False

    # Must not start with a number.
    if s[0].isdigit():
        return False

    # Check all characters.
    # Valid characters are ASCII alphanumeric or underscore, [A-Za-z0-9_].
    # If any other character is found, return False.
    return all((c == '_') or (c.isascii() and c.isalnum()) for c in s)


def is_valid_namespace(ns: str) -> bool:
    """
    Return whether one namespace string is valid for this helper module.

    The empty namespace ``''`` and the root namespace ``/`` are valid special cases. Other
    namespaces may be relative, such as ``robot1``, or absolute, such as ``/fleet/robot1``.

    Each non-empty segment must pass ``is_valid_name``. Repeated separators are rejected, so
    ``/fleet//robot1`` is invalid.
    """
    if not isinstance(ns, str):
        return False

    if ns in ('', '/'):
        return True

    # When two or more slashes are contiguous, splitting the string by '/' produces empty segments.
    # For example:
    # 'ns1//ns2'   -> ['ns1', '', 'ns2'] --> two or more '/' in a row
    # '/ns1//ns2/' -> ['', 'ns1', '', 'ns2', ''] -> the first and last are false positives.

    # To check if there are two or more '/' in a row, first remove the leading and trailing
    # slashes if present.

    # Remove EXACTLY ONE leading slash.
    if ns.startswith('/'):
        ns = ns[1:]

    # Remove EXACTLY ONE trailing slash.
    if ns.endswith('/'):
        ns = ns[:-1]

    # If ns is '//', removing leading and trailing slashes produces '', which is invalid.
    if not ns:
        return False

    # Examples at this point:
    # '/ns1//ns2/' -> 'ns1//ns2' -> ['ns1', '', 'ns2'] -> two or more '/' in a row.
    # But
    # '/ns1/ns2/'  -> 'ns1/ns2'  -> ['ns1', 'ns2'] -> OK. Leading and trailing
    # slashes are accepted, although the trailing slash is not necessary.

    items = ns.split('/')

    for item in items:
        # Empty items means two or more '/' in a row, so this is an error.
        # Technically, this check is redundant because 'is_valid_name' returns False for empty
        # strings, but keeping it here makes the namespace rule explicit.
        if not item:
            return False

        # Check the item is valid, which means ASCII alnum or underscore only, [A-Za-z0-9_].
        if not is_valid_name(item):
            return False

    return True


def read_yaml_file(yaml_file: Optional[Union[str, Path]]) -> Tuple[str, Any]:
    """
    Resolve, read, and parse one YAML file.

    ``yaml_file`` can be a regular path, a ``file://`` URI, or a ``package://`` URI accepted by
    ``resolve_file``. The returned tuple contains the resolved filesystem path and the Python value
    returned by ``yaml.safe_load``.

    If the YAML file only contains comments or whitespace, the parsed value is ``None``. The
    function raises if the path cannot be resolved, the resolved path is not a file, the file cannot
    be read as UTF-8, or the YAML content is invalid.
    """
    resolved_yaml_file = resolve_file(yaml_file)
    resolved_yaml_path = Path(resolved_yaml_file)

    if not resolved_yaml_path.is_file():
        raise FileNotFoundError(f"Path '{resolved_yaml_file}' does not point to a file")

    with resolved_yaml_path.open('r', encoding='utf-8') as f:
        data = yaml.safe_load(f)

    return (resolved_yaml_file, data)


def render_params_file(params_file: Union[str, Path], ctx: LaunchContext) -> str:
    """
    Expand launch substitutions in one ROS parameter YAML file and return a stable copy.

    ``params_file`` must already be a filesystem path. This function does not resolve
    ``package://`` or ``file://`` values.

    ``ParameterFile.evaluate()`` expands expressions such as ``$(var robot_name)`` using the
    provided launch context. The path returned by ``ParameterFile.evaluate()`` belongs to
    ``ParameterFile`` and is deleted when ``cleanup()`` runs, so this helper copies the rendered
    YAML into a new temporary file before cleaning up.

    The returned string is the path to the stable temporary copy. The caller owns that file.
    """
    params_file = Path(params_file)

    if not params_file.is_file():
        raise FileNotFoundError(f"Params file '{params_file}' does not exist.")

    output_path = _make_rendered_params_file_path()

    parameter_file = ParameterFile(str(params_file), allow_substs=True)

    try:
        evaluated_path = parameter_file.evaluate(ctx)
        output_path.write_text(Path(evaluated_path).read_text(encoding='utf-8'), encoding='utf-8')
    finally:
        parameter_file.cleanup()

    return str(output_path)


def replace_separator_in_namespace(namespace: str, new_sep: str) -> str:
    """
    Replace every ``/`` character in a valid namespace with another single character.

    ``namespace`` must be valid according to ``is_valid_namespace``. ``new_sep`` must be a
    one-character string.

    This function does not remove leading or trailing ``/`` characters. For example,
    ``/ns1/ns2/`` with ``new_sep="_"`` returns ``_ns1_ns2_``. Use ``flatten_namespace`` when the
    leading and trailing separators must be removed before replacement.
    """
    if not isinstance(namespace, str):
        raise ValueError('Namespace must be a string')

    if not isinstance(new_sep, str) or len(new_sep) != 1:
        raise ValueError('New separator must be a single character string')

    if not is_valid_namespace(namespace):
        raise RuntimeError(f"Invalid namespace '{namespace}'")

    return namespace.replace('/', new_sep)


def resolve_file(file: Optional[Union[str, Path]]) -> str:
    """
    Convert one file value into a filesystem path string.

    ``file`` can be a regular path, an absolute ``file://`` URI, or a
    ``package://<package>/<relative-path>`` URI. Regular paths and ``file://`` paths are returned
    after user-home expansion.

    The returned path is not required to exist. Callers that need an existing file must check that
    separately.

    ``package://`` paths are resolved inside the package share directory. A package URI that tries
    to escape that directory is rejected.
    """
    if file is None:
        raise NullFilePathError('File path or URI not provided')

    file = str(file).strip()

    if not file:
        raise NullFilePathError('File path or URI not provided')

    if file.startswith('package://'):
        rest = file[len('package://') :]

        if '/' not in rest:
            raise InvalidFileUriPatternError(f"Package URI must be 'package://<package>/<path>' (got: '{file}')")

        pkg, relative_file = rest.split('/', 1)

        if not pkg or not relative_file:
            raise InvalidFileUriPatternError(f"Package URI must be 'package://<package>/<path>' (got: '{file}')")

        try:
            parent_path = get_package_share_directory(pkg)
        except ValueError as e:
            raise InvalidFileUriPatternError(f"Package URI contains an invalid package name (got: '{file}')") from e

        parent_path = Path(parent_path).resolve()
        resolved_file = parent_path.joinpath(relative_file).resolve()

        if not resolved_file.is_relative_to(parent_path):
            raise InvalidFileUriPatternError(
                f"Package URI path must stay inside package share directory (got: '{file}')"
            )

        return str(resolved_file)

    if file.startswith('file://'):
        resolved_file = os.path.expanduser(file[len('file://') :])

        if not os.path.isabs(resolved_file):
            raise InvalidFileUriPatternError(f"File URI must point to an absolute path (got: '{resolved_file}')")

        return resolved_file

    if '://' in file:
        scheme = file.split('://', 1)[0]
        raise InvalidFileUriPatternError(f"Unsupported file URI scheme '{scheme}' in '{file}'")

    # If none of the special URI formats matched, return the original string with user expansion, if
    # possible.
    return os.path.expanduser(file)


def resolve_launch_action_arguments(
    str_json_arguments: Optional[str],
    default_arguments: Optional[Dict[str, Any]] = None,
    extra_rejected_arguments: Optional[set[str]] = None,
) -> Dict[str, Any]:
    """
    Convert one JSON object into keyword arguments for one launch action.

    ``str_json_arguments`` is the string value normally received from a launch argument. It must be
    empty or a JSON object. Each key is the name of one supported keyword argument from
    ``launch_ros.actions.Node``, ``launch.actions.ExecuteProcess``, or
    ``launch.actions.ExecuteLocal``.

    ``default_arguments`` contains the values chosen by the launch file. User-provided JSON values
    are merged on top of those defaults. If the JSON explicitly sets a key to ``null``, the returned
    dictionary keeps that ``None`` value instead of falling back to the default.

    ``extra_rejected_arguments`` contains extra known launch action arguments that this caller does not
    allow for this specific action. Every value in ``extra_rejected_arguments`` must be a known launch
    action argument. Unknown rejected arguments are treated as a launch-file programming error.

    The returned dictionary can be passed directly to one launch action:

    .. code-block:: python

        Node(..., **resolve_launch_action_arguments(raw_json, defaults))

    Some launch action fields are intentionally rejected because they define the action itself, not
    user-overridable runtime options. For example, ``package``, ``executable``, ``parameters``, and
    ``condition`` must be written in the launch file.

    Example JSON input:

    .. code-block:: json

        {
          "name": "robot_bridge",
          "output": "screen",
          "respawn": true,
          "remappings": [
            ["battery_state", "state/battery"],
            ["cmd_vel", "commands/velocity"]
          ]
        }

    Returned value for that JSON input:

    .. code-block:: python

        {
            "name": "robot_bridge",
            "output": "screen",
            "respawn": True,
            "remappings": [
                ("battery_state", "state/battery"),
                ("cmd_vel", "commands/velocity"),
            ],
        }
    """
    if default_arguments is None:
        default_arguments = {}
    elif not isinstance(default_arguments, dict):
        raise ValueError('default_arguments must be a dictionary')

    if extra_rejected_arguments is None:
        extra_rejected_arguments = set()
    elif not isinstance(extra_rejected_arguments, set):
        raise ValueError('extra_rejected_arguments must be a set')

    total_rejected_arguments = _REJECTED_LAUNCH_ACTION_ARGUMENTS | extra_rejected_arguments
    unknown_rejected_arguments = total_rejected_arguments - _ALL_LAUNCH_ACTION_ARGUMENTS

    if unknown_rejected_arguments:
        raise ValueError(
            'rejected launch action arguments are not known: '
            f'{sorted(unknown_rejected_arguments)}'
        )

    if str_json_arguments is None:
        return dict(default_arguments)

    if not isinstance(str_json_arguments, str):
        raise ValueError('launch_action_arguments_json_str must be a JSON string')

    str_json_arguments = str_json_arguments.strip()

    if not str_json_arguments:
        return dict(default_arguments)

    # Transform from JSON to Python types.
    try:
        arguments = json.loads(str_json_arguments)
    except json.JSONDecodeError as e:
        raise ValueError('launch_action_arguments_json_str must be valid JSON') from e

    if not isinstance(arguments, dict):
        raise ValueError('launch_action_arguments_json_str must be a JSON object')

    # Validate each argument and convert to the types expected by launch actions.
    resolved_arguments: Dict[str, Any] = {}

    for argument_name, argument_value in arguments.items():
        if not isinstance(argument_name, str) or not argument_name:
            raise ValueError('launch action argument name must be a non-empty string')

        if argument_name not in _ALL_LAUNCH_ACTION_ARGUMENTS:
            raise ValueError(f"launch action argument '{argument_name}' is not supported")

        # Some known parameters from launch_ros.actions.Node, launch.actions.ExecuteProcess,
        # and launch.actions.ExecuteLocal are rejected globally or by this caller.
        if argument_name in total_rejected_arguments:
            raise ValueError(
                f"launch action argument '{argument_name}' is not allowed; set it directly in the launch file"
            )

        match argument_name:
            case 'name' | 'namespace' | 'exec_name' | 'prefix' | 'cwd':
                resolved_arguments[argument_name] = _resolve_argument_optional_substitution(
                    argument_name, argument_value
                )
            case 'sigterm_timeout' | 'sigkill_timeout' | 'output':
                resolved_arguments[argument_name] = _resolve_argument_substitution(argument_name, argument_value)
            case 'ros_arguments' | 'arguments':
                resolved_arguments[argument_name] = _resolve_argument_optional_string_list(
                    argument_name, argument_value
                )
            case 'remappings':
                resolved_arguments[argument_name] = _resolve_argument_remappings(argument_name, argument_value)
            case 'env' | 'additional_env':
                resolved_arguments[argument_name] = _resolve_argument_optional_string_dict(
                    argument_name, argument_value
                )
            case 'shell' | 'emulate_tty' | 'cached_output' | 'log_cmd' | 'respawn':
                resolved_arguments[argument_name] = _resolve_argument_bool(argument_name, argument_value)
            case 'output_format':
                resolved_arguments[argument_name] = _resolve_argument_str(argument_name, argument_value)
            case 'respawn_delay':
                resolved_arguments[argument_name] = _resolve_argument_optional_float(argument_name, argument_value)
            case 'respawn_max_retries':
                resolved_arguments[argument_name] = _resolve_argument_int(argument_name, argument_value)
            case _:
                raise ValueError(f"launch action argument '{argument_name}' is not supported")

    return {**default_arguments, **resolved_arguments}


def resolve_name(parent_namespace: str, child_name: str) -> str:
    """
    Resolve one child namespace against one parent namespace.

    ``parent_namespace`` and ``child_name`` must both be valid namespaces according to
    ``is_valid_namespace``. ``child_name`` can be empty, relative, absolute, or ``/``.

    If ``child_name`` is relative, it is appended under ``parent_namespace``. If ``child_name`` is
    absolute, it is returned as the result and ``parent_namespace`` is ignored. If ``child_name`` is
    empty, the normalized ``parent_namespace`` is returned.

    The function removes trailing separators where needed, but it does not collapse invalid
    repeated separators such as ``//``. Invalid inputs raise ``ValueError``.
    """
    if not isinstance(parent_namespace, str) or not isinstance(child_name, str):
        raise ValueError('Arguments must be strings')

    if not is_valid_namespace(parent_namespace):
        raise ValueError(f"Invalid parent namespace '{parent_namespace}'")

    if not is_valid_namespace(child_name):
        raise ValueError(f"Invalid child name '{child_name}'")

    # If the child name starts with '/', it is an absolute namespace, so we return it as is.
    # - child_name = '/'           -> resolved_namespace = '/'
    # - child_name = '/ns1/ns2[/]' -> resolved_namespace = '/ns1/ns2'
    if child_name.startswith('/'):
        return child_name if child_name == '/' else child_name.rstrip('/')

    # If the child namespace is empty, we return the parent namespace as is (after normalizing it to
    # avoid ending with '/').
    if child_name == '':
        return parent_namespace if parent_namespace in ('', '/') else parent_namespace.rstrip('/')

    # If here, the child namespace is a relative namespace, not empty.

    # If the parent namespace is empty or '/', the parent namespace and the child namespace are
    # concatenated without adding an extra '/' between them.
    if parent_namespace in ('', '/'):
        return parent_namespace + child_name.rstrip('/')

    # If here, a possible '/' is stripped off the end of the parent namespace, and the child
    # namespace is concatenated to it with a '/' in between.
    return parent_namespace.rstrip('/') + '/' + child_name.rstrip('/')


def to_log_info_actions(messages: List[str]) -> List[LaunchDescriptionEntity]:
    """
    Convert text messages into ``LogInfo`` launch entities.

    Each non-empty string in ``messages`` becomes one ``LogInfo`` action. Empty strings are ignored.
    If ``messages`` is empty, the function returns an empty list.
    """
    if not messages:
        return []

    entities: List[LaunchDescriptionEntity] = []

    for msg in messages:
        if msg:
            entities.append(LogInfo(msg=msg))

    return entities


def to_prefix(name: str) -> str:
    """
    Convert one valid name into a prefix string.

    ``name`` must be a valid name according to ``is_valid_name``. If it already ends with ``_``, it
    is returned unchanged. Otherwise, the returned prefix is ``name`` plus one trailing ``_``.

    This helper is used when a caller wants names such as ``robot`` to become stable prefixes such
    as ``robot_`` without adding a second underscore to names that already have one.
    """
    if not isinstance(name, str):
        raise ValueError('name must be a string to create a prefix')

    if not is_valid_name(name):
        raise RuntimeError(f"'{name}' must be a non-empty string with ASCII [A-Za-z0-9_] only to create a prefix")

    # If the name already ends with '_', return it as is.
    if name.endswith('_'):
        return name
    else:
        return f'{name}_'


def _make_rendered_params_file_path() -> Path:
    """
    Create a temporary YAML file path for rendered ROS parameters.

    The file is created immediately, so concurrent launch processes cannot choose the same path.
    The caller receives the path and can overwrite the file with the rendered YAML content.
    """
    with NamedTemporaryFile(prefix='robot_params_', suffix='.yaml', delete=False) as temp_file:
        return Path(temp_file.name)


def _resolve_argument_bool(argument_name: str, argument_value: object) -> bool:
    """
    Return one launch action argument as a Python boolean.

    The input value normally comes from ``json.loads``. It must already be a JSON boolean, which
    becomes ``True`` or ``False`` in Python. Strings such as ``"true"`` are rejected because ROS 2
    launch expects a real boolean for these fields.
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

    The value normally comes from a JSON array. The array itself must be a list, and every item in
    it must be a string. ``expected_type`` is only used to make the error message match the field
    being resolved.
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

    This is used for fields such as ``respawn_delay``. JSON integers are accepted and converted to
    floats because that does not change the meaning of the value. Booleans are rejected even though
    Python treats ``bool`` as a subclass of ``int``.
    """
    if argument_value is None:
        return None

    if isinstance(argument_value, bool) or not isinstance(argument_value, (float, int)):
        raise ValueError(f"launch action argument '{argument_name}' must be a number or null")

    if not math.isfinite(float(argument_value)):
        raise ValueError(f"launch action argument '{argument_name}' must be finite")

    return float(argument_value)


def _resolve_argument_remappings(argument_name: str, argument_value: object) -> Optional[List[Tuple[str, str]]]:
    """
    Return one remappings argument as ``None`` or a list of tuple pairs.

    JSON cannot contain Python tuples, so callers write each remapping as a two-item list:
    ``["from", "to"]``. ROS 2 launch actions expect tuple pairs, so this helper converts each valid
    pair to ``("from", "to")``.

    Each pair must contain exactly two non-empty strings. ``None`` is accepted because the
    ``remappings`` launch action field is optional.
    """
    if argument_value is None:
        return None

    if not isinstance(argument_value, list):
        raise ValueError(f"launch action argument '{argument_name}' must be a JSON list")

    remappings: List[Tuple[str, str]] = []

    for index, remapping in enumerate(argument_value):
        if not isinstance(remapping, list) or len(remapping) != 2:
            raise ValueError(f"launch action argument '{argument_name}' item {index} must be a two-item list")

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

    This is used for fields such as ``output_format``. ``None`` and non-string JSON values are
    rejected because the corresponding launch action field requires a string.
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

    A value coming from ``json.loads`` cannot be a Python ``Path`` object. It also cannot be a ROS 2
    launch ``Substitution`` object, such as ``LaunchConfiguration`` or ``FindPackageShare``.
    Therefore, JSON-configured launch arguments support only the JSON-compatible part of
    ``SomeSubstitutionsType``: one string or a list of strings.
    """
    if isinstance(argument_value, str):
        return argument_value

    return _resolve_argument_string_list(argument_name, argument_value, 'string or list of strings')
