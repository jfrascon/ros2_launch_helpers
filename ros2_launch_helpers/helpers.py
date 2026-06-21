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

DEFAULT_LOGGING_OPTIONS = {
    'log-level': 'info',  # One of: 'debug', 'info', 'warn', 'error'
    'disable-stdout-logs': False,  # Whether to disable writing log messages to the console
    'disable-rosout-logs': False,  # Whether to disable writing log messages out to /rosout
    'disable-external-lib-logs': False,  # Whether to completely disable external loggers
}


LOGGING_OPTIONS_DESC = (
    'JSON object indexed by current node name. Each node entry can set "log-level", '
    '"disable-stdout-logs", "disable-rosout-logs", "disable-external-lib-logs", '
    'and custom logger levels.'
)

# `emulate_tty` makes the process see stdout and stderr as a terminal instead of a plain pipe.
# This does not change ROS behavior. It only affects console details such as colors, buffering,
# and log formatting. It is useful when a human reads the output on screen, and usually not useful
# when launch writes the output only to log files.
#
# Practical rule:
# - output='screen' -> emulate_tty=True can be useful.
# - output='both'   -> emulate_tty=True can be useful.
# - output='log'    -> emulate_tty=False is normally the better default.
DEFAULT_NODE_OPTIONS = {
    'output': 'screen',  # One of: 'screen', 'log', 'both'
    'emulate_tty': True,  # Whether to emulate a TTY for the node's stdout/stderr
    'respawn': False,  # Whether to respawn the node if it dies
    'respawn_delay': 0.0,  # Delay in seconds before respawning a node
}


NODE_OPTIONS_DESC = (
    'JSON object indexed by current node name. Each node entry can set "output", '
    '"emulate_tty", "respawn", and "respawn_delay".'
)

REMAPPINGS_DESC = (
    'JSON object indexed by current node name. Each node entry is a list of '
    'remapping strings using the "from:=to" syntax.'
)


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


def default_node_logging_options_json_str() -> str:
    return '{}'


def default_node_options_json_str() -> str:
    return '{}'


def default_node_remappings_json_str() -> str:
    return '{}'


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


def _make_rendered_params_file_path() -> Path:
    """
    Create a unique temporary YAML path for rendered ROS params.

    The file is created immediately so concurrent launches cannot choose the same path. The caller
    can overwrite it with the rendered YAML content.
    """
    with NamedTemporaryFile(prefix='robot_params_', suffix='.yaml', delete=False) as temp_file:
        return Path(temp_file.name)


def resolve_node_launch_configs(
    node_names: List[str],
    node_options: Optional[str],
    node_logging_options: Optional[str],
    node_remappings: Optional[str],
) -> Tuple[
    Dict[str, Dict[str, Union[str, bool, float]]], Dict[str, Optional[List[Tuple[str, str]]]], Dict[str, List[str]]
]:
    if not isinstance(node_names, list):
        raise ValueError('node_names must be a list of node names')

    for index, node_name in enumerate(node_names):
        _NodeLaunchConfig.validate_node_name(node_name, f'node_names item {index}')

    config = _NodeLaunchConfig.from_json_strings(
        node_options=node_options, node_logging_options=node_logging_options, node_remappings=node_remappings
    )

    node_options_by_name: Dict[str, Dict[str, Union[str, bool, float]]] = {}
    remappings_by_name: Dict[str, Optional[List[Tuple[str, str]]]] = {}
    ros_arguments_by_name: Dict[str, List[str]] = {}

    for node_name in node_names:
        node_options_by_name[node_name] = config.options_for(node_name)
        remappings_by_name[node_name] = config.remappings_for(node_name)
        ros_arguments_by_name[node_name] = config.logging_ros_arguments_for(node_name)

    return node_options_by_name, remappings_by_name, ros_arguments_by_name


class _NodeLaunchConfig:
    """
    Store node launch overrides from the three JSON launch arguments.

    Each JSON value is a map indexed by the effective node name used by the launch file.
    """

    _LOG_LEVELS = ('debug', 'info', 'warn', 'error')
    _CUSTOM_LOG_LEVELS = ('debug', 'info', 'warn', 'error', 'fatal')
    _LOGGING_BOOLEAN_KEYS = ('disable-stdout-logs', 'disable-rosout-logs', 'disable-external-lib-logs')
    _OUTPUT_VALUES = ('screen', 'log', 'both')

    def __init__(
        self, node_options: Dict[str, Any], node_logging_options: Dict[str, Any], node_remappings: Dict[str, Any]
    ) -> None:
        self._node_options = node_options
        self._node_logging_options = node_logging_options
        self._node_remappings = node_remappings

    @classmethod
    def from_json_strings(
        cls, node_options: Optional[str], node_logging_options: Optional[str], node_remappings: Optional[str]
    ) -> '_NodeLaunchConfig':
        return cls(
            node_options=cls._parse_json_map(node_options, 'node_options'),
            node_logging_options=cls._parse_json_map(node_logging_options, 'node_logging_options'),
            node_remappings=cls._parse_json_map(node_remappings, 'node_remappings'),
        )

    def logging_ros_arguments_for(self, current_node_name: str) -> List[str]:
        self.validate_node_name(current_node_name, 'current_node_name')
        logging_options: Dict[str, Union[str, bool]] = DEFAULT_LOGGING_OPTIONS.copy()
        node_logging_options = self._entry_for(self._node_logging_options, current_node_name, 'node_logging_options')

        if node_logging_options is None:
            return self._logging_options_to_ros_args(logging_options)

        if not isinstance(node_logging_options, dict):
            raise ValueError(f"node_logging_options entry for node '{current_node_name}' must be a JSON object")

        for key, value in node_logging_options.items():
            if not isinstance(key, str) or not key:
                raise ValueError(f"node_logging_options for node '{current_node_name}' has an invalid key")

            if key == 'log-level':
                logging_options[key] = self._validate_log_level(value, current_node_name, key)
            elif key in self._LOGGING_BOOLEAN_KEYS:
                logging_options[key] = self._validate_bool(value, 'node_logging_options', current_node_name, key)
            else:
                logging_options[key] = self._validate_custom_log_level(value, current_node_name, key)

        return self._logging_options_to_ros_args(logging_options)

    def options_for(self, current_node_name: str) -> Dict[str, Union[str, bool, float]]:
        self.validate_node_name(current_node_name, 'current_node_name')
        node_options: Dict[str, Union[str, bool, float]] = DEFAULT_NODE_OPTIONS.copy()
        node_options_overrides = self._entry_for(self._node_options, current_node_name, 'node_options')

        if node_options_overrides is None:
            return node_options

        if not isinstance(node_options_overrides, dict):
            raise ValueError(f"node_options entry for node '{current_node_name}' must be a JSON object")

        for key, value in node_options_overrides.items():
            if key not in DEFAULT_NODE_OPTIONS:
                raise ValueError(f"node_options for node '{current_node_name}' has unknown key '{key}'")

            match key:
                case 'output':
                    node_options[key] = self._validate_output(value, current_node_name)
                case 'emulate_tty':
                    node_options[key] = self._validate_bool(value, 'node_options', current_node_name, key)
                case 'respawn':
                    node_options[key] = self._validate_bool(value, 'node_options', current_node_name, key)
                case 'respawn_delay':
                    node_options[key] = self._validate_number(value, 'node_options', current_node_name, key)
                case _:  # Should never happen because unknown keys are rejected above.
                    pass

        return node_options

    def remappings_for(self, current_node_name: str) -> Optional[List[Tuple[str, str]]]:
        self.validate_node_name(current_node_name, 'current_node_name')
        node_remappings = self._entry_for(self._node_remappings, current_node_name, 'node_remappings')

        if node_remappings is None:
            return None

        if not isinstance(node_remappings, list):
            raise ValueError(f"node_remappings entry for node '{current_node_name}' must be a JSON list")

        remappings: List[Tuple[str, str]] = []
        for index, remapping in enumerate(node_remappings):
            if not isinstance(remapping, str):
                raise ValueError(f"node_remappings item {index} for node '{current_node_name}' must be a string")

            try:
                original_topic, new_topic = remapping.split(':=', maxsplit=1)
            except ValueError as e:
                raise ValueError(
                    f"node_remappings item {index} for node '{current_node_name}' must use 'from:=to' syntax"
                ) from e

            if not original_topic:
                raise ValueError(
                    f"node_remappings item {index} for node '{current_node_name}' must have a non-empty 'from'"
                )

            if not new_topic:
                raise ValueError(
                    f"node_remappings item {index} for node '{current_node_name}' must have a non-empty 'to'"
                )

            remappings.append((original_topic, new_topic))

        return remappings

    @staticmethod
    def _entry_for(values: Dict[str, Any], current_node_name: str, field_name: str) -> Optional[Any]:
        try:
            return values[current_node_name]
        except KeyError:
            return None
        except TypeError as e:
            raise ValueError(f'{field_name} must be a JSON object') from e

    @staticmethod
    def _logging_options_to_ros_args(logging_options: Dict[str, Any]) -> List[str]:
        args = []

        for key, value in logging_options.items():
            if key == 'log-level':
                if value:
                    args.extend(['--log-level', value])
            elif key in _NodeLaunchConfig._LOGGING_BOOLEAN_KEYS:
                if value:
                    args.append(f'--{key}')
            elif value:
                args.extend(['--log-level', f'{key}:={value}'])

        return args

    @staticmethod
    def _parse_json_map(value: Optional[str], field_name: str) -> Dict[str, Any]:
        if value is None:
            return {}

        if not isinstance(value, str):
            raise ValueError(f'{field_name} must be a JSON string')

        value = value.strip()
        if not value:
            return {}

        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as e:
            raise ValueError(f'{field_name} must be valid JSON') from e

        if not isinstance(parsed, dict):
            raise ValueError(f'{field_name} must be a JSON object')

        return parsed

    @staticmethod
    def _validate_bool(value: Any, field_name: str, current_node_name: str, key: str) -> bool:
        if not isinstance(value, bool):
            raise ValueError(f"{field_name} '{key}' for node '{current_node_name}' must be a boolean")

        return value

    @classmethod
    def _validate_custom_log_level(cls, value: Any, current_node_name: str, key: str) -> str:
        level = cls._validate_string(value, 'node_logging_options', current_node_name, key).lower()
        if level not in cls._CUSTOM_LOG_LEVELS:
            raise ValueError(
                f"node_logging_options custom logger '{key}' for node '{current_node_name}' has invalid level '{value}'"
            )

        return level

    @classmethod
    def _validate_log_level(cls, value: Any, current_node_name: str, key: str) -> str:
        level = cls._validate_string(value, 'node_logging_options', current_node_name, key).lower()
        if level not in cls._LOG_LEVELS:
            raise ValueError(f"node_logging_options '{key}' for node '{current_node_name}' has invalid level '{value}'")

        return level

    @staticmethod
    def validate_node_name(node_name: str, field_name: str) -> None:
        if not isinstance(node_name, str) or not node_name:
            raise ValueError(f'{field_name} must be a non-empty string')

        if not is_valid_name(node_name):
            raise ValueError(f"{field_name} must be ASCII [A-Za-z0-9_] only: '{node_name}'")

    @staticmethod
    def _validate_number(value: Any, field_name: str, current_node_name: str, key: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (float, int)):
            raise ValueError(f"{field_name} '{key}' for node '{current_node_name}' must be a number")

        numeric_value = float(value)
        if not math.isfinite(numeric_value) or numeric_value < 0:
            raise ValueError(
                f"{field_name} '{key}' for node '{current_node_name}' must be a finite number "
                'greater than or equal to 0'
            )

        return numeric_value

    @classmethod
    def _validate_output(cls, value: Any, current_node_name: str) -> str:
        output = cls._validate_string(value, 'node_options', current_node_name, 'output')
        if output not in cls._OUTPUT_VALUES:
            raise ValueError(f"node_options 'output' for node '{current_node_name}' has invalid value '{value}'")

        return output

    @staticmethod
    def _validate_string(value: Any, field_name: str, current_node_name: str, key: str) -> str:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{field_name} '{key}' for node '{current_node_name}' must be a non-empty string")

        return value


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
