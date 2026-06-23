import json
import math
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml
from ament_index_python.packages import get_package_share_directory
from launch import LaunchContext, LaunchDescriptionEntity
from launch.actions import LogInfo
from launch_ros.parameter_descriptions import ParameterFile

LAUNCH_ACTION_ARGUMENTS_DESC = (
    'JSON string containing supported arguments for one launch_ros.actions.Node, '
    'launch.actions.ExecuteProcess, or launch.actions.ExecuteLocal action.'
)

_REJECTED_LAUNCH_ACTION_ARGUMENTS = {
    'package',
    'executable',
    'namespace',
    'parameters',
    'cmd',
    'process_description',
    'on_exit',
    'condition',
}


class FileResolutionError(ValueError):
    """
    Base exception for failures while converting a file path or URI into a filesystem path.
    """


class NullFilePathError(FileResolutionError):
    """
    Raised when a file path or URI is required but the provided value is empty.
    """


class InvalidFileUriPatternError(FileResolutionError):
    """
    Raised when a file URI does not follow one of the URI formats accepted by ``resolve_file``.
    """


def compute_global_namespace(namespace: str) -> str:
    """
    Resolve one launch namespace argument as an absolute namespace.

    This helper contains only the namespace rule. Launch actions are responsible for reading the
    input value from the launch context and writing the result back to the launch context.
    """
    return resolve_name('/', namespace)


def compute_robot_namespace(namespace: str, robot_name: str) -> str:
    """
    Resolve one robot name inside a parent namespace.

    This helper keeps the naming rule independent from ROS launch runtime concerns.
    """
    return resolve_name(namespace, robot_name)


def compute_robot_prefix(robot_name: str) -> str:
    """
    Convert one robot name into the prefix used for frame and topic names.
    """
    return to_prefix(robot_name)


def default_launch_action_arguments_json_str() -> str:
    return '{}'


def flatten_namespace(namespace: str, new_sep: str) -> str:
    """
    Flatten a namespace into a non-hierarchical string.

    The leading and trailing '/' are removed before replacing inner '/' separators with `new_sep`.
    This means the root namespace '/' and the empty namespace '' are both represented as ''.
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
    Build the parameters field with a base parameter file and optional files overlaying it.
    :param params_file: Path to the base YAML file with ros__parameters (required).
    :param overlay_params_file_list: Comma-separated list of YAML files to overlay (optional,
        default: '').
    :return: List of ParameterFile objects to be passed to the 'parameters' field of a Node.
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
    Validate characters of a string segment.

    Returns:
        - True  -> all characters are valid (ASCII alnum or underscore) and length is valid.
        - False -> at least one invalid character found.

    Notes:
        - Rules match ROS 2 node name validation (Humble):
          https://github.com/ros2/rmw/blob/humble/rmw/src/validate_node_name.c
          https://github.com/ros2/rclpy/blob/humble/rclpy/src/rclpy/names.cpp
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
    Validate a namespace string.

    Rules:
    - '' and '/' are permitted as special cases (root/empty).
    - Reject empty segments (no '//' allowed at any position).
    - Each non-empty segment must be valid, i.e., ASCII alnum or underscore only, [A-Za-z0-9_].
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
    Read and parse a YAML file, returning both the resolved path and the loaded object.

    :param yaml_file: Path or URI (package://, file://, or regular path) to the YAML file.
    :return: Tuple ``(resolved_path, data)`` where ``data`` is the parsed YAML object; it can be
        ``None`` when the file contains only comments/whitespace.
    :raises NullFilePathError: If no YAML file path or URI is provided.
    :raises InvalidFileUriPatternError: If a URI does not follow one of the supported file URI
        formats.
    :raises PackageNotFoundError: If the package in a ``package://`` URI is not in the ROS package
        index.
    :raises FileNotFoundError: If the resolved path does not point to a file.
    :raises yaml.YAMLError: If the YAML syntax is invalid.
    :raises OSError: If the file cannot be read.
    :raises UnicodeDecodeError: If the file is not valid UTF-8.
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
    Render one ROS parameter YAML file.

    `params_file` must be a resolved filesystem path.

    `ParameterFile.evaluate()` uses the launch_ros YAML substitution engine. With
    `allow_substs=True`, it expands expressions such as `$(var robot_name)` using the current
    launch context and returns the path to a temporary expanded YAML file.

    This function copies the temporary expanded YAML to a new temporary file because
    `ParameterFile.cleanup()` deletes the file returned by `ParameterFile.evaluate()`.

    :param params_file: Resolved filesystem path to the ROS parameter YAML file.
    :param ctx: Launch context used to evaluate substitutions in the parameter YAML file.
    :return: Filesystem path to the rendered YAML file.
    :raises FileNotFoundError: If the resolved params file path does not point to a file.
    :raises OSError: If the output directory cannot be created, or if the temporary or output file
        cannot be read or written.
    :raises UnicodeDecodeError: If the evaluated temporary YAML file is not valid UTF-8.
    :raises Exception: If ``launch_ros.parameter_descriptions.ParameterFile.evaluate`` fails while
        expanding substitutions.
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
    Replace each '/' separator in a namespace with `new_sep`.

    This function performs the replacement on the namespace string itself. It does not remove a
    leading '/' or a trailing '/'. For example, '/' becomes `new_sep`, and '/ns1/ns2/' becomes
    '<new_sep>ns1<new_sep>ns2<new_sep>'.
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
    Resolve one regular path, ``file://`` URI, or ``package://`` URI to a filesystem path.

    Regular paths are returned after ``~`` expansion. The path does not have to exist; callers that
    need an existing file should check that separately after resolution.

    :param file: Regular path, ``file://`` URI, or ``package://<package>/<path>`` URI.
    :return: Resolved filesystem path.
    :raises NullFilePathError: If no file path or URI is provided.
    :raises InvalidFileUriPatternError: If a URI does not follow one of the supported file URI
        formats.
    :raises PackageNotFoundError: If the package in a ``package://`` URI is not in the ROS package
        index.
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
    str_json_arguments: Optional[str], default_arguments: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Parse one JSON object with arguments for one launch action.

    The JSON root must be an object whose fields are supported arguments from ``Node``,
    ``ExecuteProcess``, or ``ExecuteLocal``. The returned dictionary can be passed to one launch
    action with ``**arguments``.

    ``default_arguments`` is merged before the JSON object. The launch file owns those default
    arguments because it knows which action is being created. JSON values override default
    arguments, including ``null`` values.

    Example input:

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

    The returned value for the example above is:

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

    if not isinstance(default_arguments, dict):
        raise ValueError('default_arguments must be a dictionary')

    if str_json_arguments is None:
        return dict(default_arguments)

    if not isinstance(str_json_arguments, str):
        raise ValueError('launch_action_arguments_json_str must be a JSON string')

    str_json_arguments = str_json_arguments.strip()

    if not str_json_arguments:
        return dict(default_arguments)

    try:
        arguments = json.loads(str_json_arguments)
    except json.JSONDecodeError as e:
        raise ValueError('launch_action_arguments_json_str must be valid JSON') from e

    if not isinstance(arguments, dict):
        raise ValueError('launch_action_arguments_json_str must be a JSON object')

    validated_arguments = _validate_launch_action_arguments(arguments)

    return {**default_arguments, **validated_arguments}


def resolve_name(parent_namespace: str, child_name: str) -> str:
    """
    Resolve `child_name` with respect to `parent_namespace`.

    `child_name` can be a single relative name segment such as `robot`, a relative namespace
    expression such as `ns1/ns2`, an absolute namespace expression such as `/ns1/ns2`, or an empty
    string.

    If `child_name` is relative, append it to `parent_namespace`.
    If `child_name` is absolute, return `child_name` and ignore `parent_namespace`.
    If `child_name` is empty, return `parent_namespace`.

    Both inputs must be valid namespace strings. This function does not collapse repeated `/`; it
    rejects invalid namespaces instead.
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
    Convert a list of text messages into launch LogInfo entities.

    Empty messages are ignored.
    """
    if not messages:
        return []

    entities: List[LaunchDescriptionEntity] = []

    for msg in messages:
        if msg:
            entities.append(LogInfo(msg=msg))

    return entities


def to_prefix(name: str) -> str:
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
    Create a unique temporary YAML path for rendered ROS params.

    The file is created immediately so concurrent launches cannot choose the same path. The caller
    can overwrite it with the rendered YAML content.
    """
    with NamedTemporaryFile(prefix='robot_params_', suffix='.yaml', delete=False) as temp_file:
        return Path(temp_file.name)


def _validate_bool(argument_name: str, argument_value: object) -> bool:
    """
    Validate one JSON boolean argument.

    This is used for arguments such as ``emulate_tty`` and ``respawn``. JSON must contain
    ``true`` or ``false``. Strings such as ``"true"`` are rejected.
    """
    if not isinstance(argument_value, bool):
        raise ValueError(f"launch action argument '{argument_name}' must be a boolean")

    return argument_value


def _validate_int(argument_name: str, argument_value: object) -> int:
    """
    Validate one integer argument.

    This is used for ``respawn_max_retries``. JSON must contain an integer number. Booleans are
    rejected even though Python treats ``bool`` as a subclass of ``int``.
    """
    # In Python a boolean is a subclass of an integer, therefore
    # isinstance(True, int) return True, therefore, if we want to allow int value to be passed as a
    # value for respawn_max_retries, we need to check if the value is a boolean first and reject it.
    if isinstance(argument_value, bool) or not isinstance(argument_value, int):
        raise ValueError(f"launch action argument '{argument_name}' must be an integer")

    return argument_value


def _validate_launch_action_arguments(arguments: Dict[str, object]) -> Dict[str, Any]:
    """
    Validate the arguments object for one launch action.

    Continuing the example from ``resolve_launch_action_arguments``, this function checks each
    field in the JSON object, rejects fields that must stay in the launch file, and converts
    ``remappings`` from JSON lists into Python tuples.
    """
    validated_arguments: Dict[str, Any] = {}

    for argument_name, argument_value in arguments.items():
        if not isinstance(argument_name, str) or not argument_name:
            raise ValueError('launch action argument name must be a non-empty string')

        if argument_name in _REJECTED_LAUNCH_ACTION_ARGUMENTS:
            raise ValueError(f"launch action argument '{argument_name}' must be written in the launch file")

        match argument_name:
            case 'name' | 'exec_name' | 'prefix' | 'cwd':
                validated_arguments[argument_name] = _validate_optional_some_substitutions_type(
                    argument_name, argument_value
                )
            case 'sigterm_timeout' | 'sigkill_timeout' | 'output':
                validated_arguments[argument_name] = _validate_some_substitutions_type(argument_name, argument_value)
            case 'ros_arguments' | 'arguments':
                validated_arguments[argument_name] = _validate_optional_string_list(argument_name, argument_value)
            case 'remappings':
                validated_arguments[argument_name] = _validate_optional_remappings(argument_name, argument_value)
            case 'env' | 'additional_env':
                validated_arguments[argument_name] = _validate_optional_env_map(argument_name, argument_value)
            case 'shell' | 'emulate_tty' | 'cached_output' | 'log_cmd' | 'respawn':
                validated_arguments[argument_name] = _validate_bool(argument_name, argument_value)
            case 'output_format':
                validated_arguments[argument_name] = _validate_string(argument_name, argument_value)
            case 'respawn_delay':
                validated_arguments[argument_name] = _validate_optional_float(argument_name, argument_value)
            case 'respawn_max_retries':
                validated_arguments[argument_name] = _validate_int(argument_name, argument_value)
            case _:
                raise ValueError(f"launch action argument '{argument_name}' is not supported")

    return validated_arguments


def _validate_optional_env_map(argument_name: str, argument_value: object) -> Optional[Dict[str, str]]:
    """
    Validate Optional[Dict[SomeSubstitutionsType, SomeSubstitutionsType]] = None.

    This is used for ``env`` and ``additional_env``. JSON may contain an object or ``null``.
    ``null`` is returned as Python ``None`` because the original ROS 2 type is optional.
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

    return dict(argument_value)


def _validate_optional_float(argument_name: str, argument_value: object) -> Optional[float]:
    """
    Validate Optional[float] = None.

    This is used for ``respawn_delay``. JSON may contain a number or ``null``. Booleans are
    rejected even though Python treats ``bool`` as a subclass of ``int``.
    """
    if argument_value is None:
        return None

    # In Python a boolean is a subclass of an integer, therefore
    # isinstance(True, int) return True, therefore, if we want to allow int or float value to be
    # passed as a value for respawn_delay, we need to check if the value is a boolean first and
    # reject it.
    if isinstance(argument_value, bool) or not isinstance(argument_value, (float, int)):
        raise ValueError(f"launch action argument '{argument_name}' must be a number or null")

    numeric_value = float(argument_value)

    if not math.isfinite(numeric_value):
        raise ValueError(f"launch action argument '{argument_name}' must be finite")

    return numeric_value


def _validate_optional_remappings(argument_name: str, argument_value: object) -> Optional[List[Tuple[str, str]]]:
    """
    Validate Optional[SomeRemapRules] = None.

    JSON may contain a list of remapping pairs or ``null``. ``null`` is returned as Python
    ``None`` because the original ROS 2 type is optional.
    """
    if argument_value is None:
        return None

    if not isinstance(argument_value, list):
        raise ValueError(f"launch action argument '{argument_name}' must be a JSON list")

    remappings: List[Tuple[str, str]] = []

    # remappings: Optional[SomeRemapRules] = None,
    # (type) SomeRemapRules = Iterable[Tuple[str | Path | Substitution | Iterable[str | Path |
    # Substitution], str | Path | Substitution | Iterable[str | Path | Substitution]]]
    # Basically a remapping is a list of tuples, where each tuple is a pair of strings.
    # In json, we do not have tuples, so we use a list of lists instead.
    # The list of lists is converted into a list of tuples in this function.
    # Each inner list must have exactly two items, both of which must be non-empty strings.
    # [[from1, to2], [from2, to2]] -> [(from1, to1), (from2, to2)]

    for index, remapping in enumerate(argument_value):
        if not isinstance(remapping, list) or len(remapping) != 2:
            raise ValueError(f"launch action argument 'remappings' item {index} must be a two-item list")

        original_name, new_name = remapping

        if not isinstance(original_name, str) or not original_name:
            raise ValueError(f"launch action argument 'remappings' item {index} must have a non-empty string source")

        if not isinstance(new_name, str) or not new_name:
            raise ValueError(f"launch action argument 'remappings' item {index} must have a non-empty string target")

        remappings.append((original_name, new_name))

    return remappings


def _validate_optional_some_substitutions_type(
    argument_name: str, argument_value: object
) -> Optional[Union[str, List[str]]]:
    """
    Validate Optional[SomeSubstitutionsType] = None.

    JSON may contain a string, a list of strings, or ``null``. ``null`` is returned as Python
    ``None`` because the original ROS 2 type is optional.
    """
    if argument_value is None:
        return None

    return _validate_some_substitutions_type(argument_name, argument_value)


def _validate_optional_string_list(argument_name: str, argument_value: object) -> Optional[List[str]]:
    """
    Validate Optional[Iterable[SomeSubstitutionsType]] = None.

    This is used for ``ros_arguments`` and ``arguments``. JSON may contain a list of strings or
    ``null``. ``null`` is returned as Python ``None`` because the original ROS 2 type is optional.
    """
    if argument_value is None:
        return None

    return _validate_string_list(argument_name, argument_value)


def _validate_some_substitutions_type(argument_name: str, argument_value: object) -> Union[str, List[str]]:
    """
    Validate the JSON subset accepted for ROS 2 ``SomeSubstitutionsType``.

    In ROS 2 Jazzy, the type is defined in:

    ``/opt/ros/jazzy/lib/python3.12/site-packages/launch/some_substitutions_type.py``

    The definition is:

    .. code-block:: python

        SomeSubstitutionsType = Union[
            Text,
            Path,
            Substitution,
            Iterable[Union[Text, Path, Substitution]],
        ]

    JSON cannot represent Python ``Path`` objects. JSON also cannot represent ROS 2 launch
    ``Substitution`` objects, such as ``LaunchConfiguration`` or ``FindPackageShare``. This helper
    therefore accepts only the part of that type that can be written naturally in JSON: a string or
    a list of strings.

    Users should normally write a single JSON string. For example, write ``"screen"`` instead of
    ``["scr", "een"]``. The list form exists only because ROS 2 launch also accepts a list for
    ``SomeSubstitutionsType``. In this helper, the list may contain only strings. When ROS 2 launch
    resolves that list, it joins the strings into one final string. For example,
    ``["scr", "een"]`` becomes ``"screen"``.
    """
    if isinstance(argument_value, str):
        return argument_value

    return _validate_string_list(argument_name, argument_value, expected_type='string or list of strings')


def _validate_string(argument_name: str, argument_value: object) -> str:
    """
    Validate one string argument.

    This is used for fields such as ``output_format``. JSON must contain a string value.
    """
    if not isinstance(argument_value, str):
        raise ValueError(f"launch action argument '{argument_name}' must be a string")

    return argument_value


def _validate_string_list(
    argument_name: str, argument_value: object, expected_type: str = 'list of strings'
) -> List[str]:
    """
    Validate one list of strings.

    This is used for arguments such as ``ros_arguments`` and ``arguments``. JSON must contain a
    list, and every item in that list must be a string. ``expected_type`` is only used in the error
    message when the value is not a list.
    """
    if not isinstance(argument_value, list):
        raise ValueError(f"launch action argument '{argument_name}' must be a {expected_type}")

    validated_items: List[str] = []

    for index, item in enumerate(argument_value):
        if not isinstance(item, str):
            raise ValueError(f"launch action argument '{argument_name}' item {index} must be a string")

        validated_items.append(item)

    return validated_items
