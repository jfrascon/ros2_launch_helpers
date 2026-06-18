import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml
from ament_index_python.packages import get_package_share_directory

# from benedict import benedict
from launch import LaunchContext, LaunchDescriptionEntity
from launch.actions import LogInfo, SetLaunchConfiguration
from launch.substitutions import LaunchConfiguration
from launch_ros.parameter_descriptions import ParameterFile

DEFAULT_LOGGING_OPTIONS = {
    'log-level': 'info',  # One of: 'debug', 'info', 'warn', 'error'
    'disable-stdout-logs': False,  # Whether to disable writing log messages to the console
    'disable-rosout-logs': False,  # Whether to disable writing log messages out to /rosout
    'disable-external-lib-logs': False,  # Whether to completely disable the use of an external logger
}


LOGGING_OPTIONS_DESC = (
    'JSON object indexed by current node name. Each node entry can set "log-level", '
    '"disable-stdout-logs", "disable-rosout-logs", "disable-external-lib-logs", '
    'and custom logger levels.'
)

DEFAULT_NODE_OPTIONS = {
    'output': 'screen',  # One of: 'screen', 'log', 'both'
    'emulate_tty': True,  # Whether to emulate a TTY for the node's stdout/stderr (usually True for 'screen' or 'both')
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


################################################################################
# Functions that can be passed as argument to OpaqueFunction.
# (Context access available)
################################################################################


def set_global_namespace(
    ctx: LaunchContext, namespace_key: str = 'namespace', output_namespace_key: str = 'namespace'
) -> list[LaunchDescriptionEntity]:
    """
    Convert the namespace stored in `namespace_key` to an absolute namespace and write it back to
    `namespace_key`.
    """
    # For example:
    # - 'a/b/c' is converted to /a/b/c, note the first '/' character.
    # - '' is converted to '/'.

    if not is_valid_name(output_namespace_key):
        raise RuntimeError(f"The output namespace key must be ASCII [A-Za-z0-9_] only: '{output_namespace_key}'")

    namespace = LaunchConfiguration(namespace_key).perform(ctx)
    return [SetLaunchConfiguration(output_namespace_key, resolve_name('/', namespace))]


def set_robot_namespace(
    ctx: LaunchContext,
    namespace_key: str = 'namespace',
    robot_name_key: str = 'robot_name',
    robot_namespace_key: str = 'robot_namespace',
) -> list[LaunchDescriptionEntity]:
    """
    Set the `robot_namespace_key` in the launch context by resolving the `robot_name` into the
    `namespace`.
    """
    namespace = LaunchConfiguration(namespace_key).perform(ctx)
    robot_name = LaunchConfiguration(robot_name_key).perform(ctx)
    return [SetLaunchConfiguration(robot_namespace_key, resolve_name(namespace, robot_name))]


def set_robot_prefix(
    ctx: LaunchContext, robot_name_key: str = 'robot_name', robot_prefix_key: str = 'robot_prefix'
) -> list[LaunchDescriptionEntity]:
    """
    Set the 'robot_prefix_key' in the launch context by converting the 'robot_name' into a prefix format
    (e.g., 'robot1' -> 'robot1_')
    """
    robot_name = LaunchConfiguration(robot_name_key).perform(ctx)

    if not is_valid_name(robot_name):
        raise RuntimeError(f"The robot's name must be ASCII [A-Za-z0-9_] only: '{robot_name}'")

    return [SetLaunchConfiguration(robot_prefix_key, to_prefix(robot_name))]


# def set_robot_odometry_frame(
#     ctx: LaunchContext,
#     local_odometry_frame_key: str = 'local_odometry_frame',
#     robot_prefix_key: str = 'robot_prefix',
#     odometry_frame_key: str = 'odometry_frame',
# ) -> list[LaunchDescriptionEntity]:
#     """
#     Set the `odometry_frame_key` in the launch context joining the `robot_prefix` and the `odometry_frame`.
#     """
#     local_odometry_frame = LaunchConfiguration(local_odometry_frame_key).perform(ctx)
#     robot_prefix = LaunchConfiguration(robot_prefix_key).perform(ctx)
#     return [SetLaunchConfiguration(odometry_frame_key, f'{robot_prefix}{local_odometry_frame}')]


# def set_robot_base_frame(
#     ctx: LaunchContext,
#     local_base_frame: str = 'base_link',
#     robot_prefix_key: str = 'robot_prefix',
#     base_frame_key: str = 'base_frame',
# ) -> list[LaunchDescriptionEntity]:
#     """
#     Set the `robot_base_frame_key` in the launch context joining the `robot_prefix` and the
#     `base_frame`.
#     """
#     robot_prefix = LaunchConfiguration(robot_prefix_key).perform(ctx)
#     return [SetLaunchConfiguration(base_frame_key, f'{robot_prefix}{local_base_frame}')]

################################################################################
# Functions without access to the context
################################################################################


def default_node_logging_options_json_str() -> str:
    return '{}'


def default_node_options_json_str() -> str:
    return '{}'


def default_node_remappings_json_str() -> str:
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
    :param overlay_params_file_list: Comma-separated list of YAML files to overlay (optional, default: '').
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
        - None  -> input is empty; considered 'not evaluable' at this level.

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
    if ns in ('', '/'):
        return True

    # When two or more slashes are contiguos, when you split the string by '/', you get empty segments.
    # For example:
    # 'ns1//ns2'   -> ['ns1', '', 'ns2'] --> two or more '/' in a row
    # '/ns1//ns2/' -> ['', 'ns1', '', 'ns2', ''] -> the first and last are false positives.

    # In order to check if there are two or more '/' in a row, we need to first remove the leading and trailing slashes
    # if present.

    # Remove EXACTLY ONE leading slash.
    if ns.startswith('/'):
        ns = ns[1:]

    # Remove EXACTLY ONE trailing slash.
    if ns.endswith('/'):
        ns = ns[:-1]

    # If ns is '//', after removing leading and trailing slashes, it becomes '', which is an invalid namespace.
    if not ns:
        return False

    # Examples at this point:
    # '/ns1//ns2/' -> 'ns1//ns2' -> ['ns1', '', 'ns2'] -> two or more '/' in a row, this is an error.
    # But
    # '/ns1/ns2/'  -> 'ns1/ns2'  -> ['ns1', 'ns2']  -> OK. The leadind and trailing slashes are OK, although the
    #                                                      trailing slash is not necessary.

    items = ns.split('/')

    for item in items:
        # Empty items means two or more '/' in a row, so this is an error.
        # Technically, we could have removed this 'if not item' check, since 'is_valid_name' would return False for
        # empty strings, but this way we can provide a more specific error message.
        if not item:
            return False

        # Check the item is in valid, which means ASCII alnum or underscore only, [A-Za-z0-9_].
        if not is_valid_name(item):
            return False

    return True


# def merge_yaml_maps_strict(
#     defaults: Mapping[str, Any],
#     override: Mapping[str, Any],
#     keypath_sep: str = '§',
#     flat_sep: str = '.',
#     allow_none: bool = True,
#     numeric_compat: bool = False,
# ) -> Tuple[Dict[str, Any], List, List]:
#     def same_type(a, b, numeric_compat: bool = False) -> bool:
#         """
#         Return True if 'b' is allowed to override 'a' according to type rules.

#         Rules:
#         1) Exact type match passes (type(a) is type(b)).
#         2) If numeric_compat is True, allow int <-> float interchange,
#             but never allow bool (since bool is a subclass of int in Python).
#         3) Otherwise, types must match exactly.

#         Examples:
#         same_type(3, 7) -> True
#         same_type(3, 7.0) -> False
#         same_type(3, 7.0, numeric_compat=True) -> True
#         same_type(True, 1, numeric_compat=True) -> False  # bool explicitly excluded
#         same_type([1], [2]) -> True
#         same_type([1], "x") -> False
#         """
#         if type(a) is type(b):
#             return True

#         # Optional numeric compatibility (int <-> float), but exclude bool explicitly.
#         # 'numbers.Real' captures int and float and bool, so we must filter bool out.
#         if numeric_compat and isinstance(a, numbers.Real) and isinstance(b, numbers.Real):
#             return not isinstance(a, bool) and not isinstance(b, bool)

#         # All other combinations are not allowed.
#         return False

#     # Wrap with benedict using a keypath separator that doesn't appear in keys
#     d = benedict(defaults, keypath_separator=keypath_sep)
#     o = benedict(override, keypath_separator=keypath_sep)

#     # Flatten both with dotted paths (independent from keypath sep)
#     d_flatten = d.flatten(flat_sep)
#     o_flatten = o.flatten(flat_sep)

#     merged_flat = {}
#     applied = []
#     ignored = []

#     # Walk only default keys -> overlay strict
#     for dk, dv in d_flatten.items():
#         if dk in o_flatten:
#             ov = o_flatten[dk]
#             # Allow None values if specified
#             if ov is None:
#                 if allow_none:
#                     merged_flat[dk] = None
#                     applied.append(dk)
#                 else:
#                     # Treat None as 'missing override': inherit default and record as ignored
#                     merged_flat[dk] = dv
#                     ignored.append(dk)
#                 continue

#             if not same_type(dv, ov, numeric_compat):
#                 raise TypeError(f'{dk}: type mismatch (default={type(dv).__name__}, override={type(ov).__name__})')

#             # Lists replace lists; scalars replace scalars – both covered by type check
#             merged_flat[dk] = ov
#             applied.append(dk)
#         else:
#             merged_flat[dk] = dv

#     # Collect extras from override (ignored by design)
#     for ok in o_flatten.keys():
#         if ok not in d_flatten:
#             ignored.append(ok)

#     # Rebuild nested dict
#     nested = benedict(merged_flat, keypath_separator=keypath_sep).unflatten(separator=flat_sep)

#     return nested, applied, ignored


def render_params_file(
    params_file: Union[str, Path], rendered_params_file: Union[str, Path], ctx: LaunchContext
) -> None:
    """
    Render one ROS parameter YAML file.

    `params_file` may be a normal filesystem path, a `file://` URI, or a `package://<package>/<path>` URI.
    The rendered YAML is written to the required `rendered_params_file`.

    `ParameterFile.evaluate()` uses the launch_ros YAML substitution engine. With `allow_substs=True`, it expands
    expressions such as `$(var robot_name)` using the current launch context and returns the path to a temporary
    expanded YAML file.

    This function copies the temporary expanded YAML to `rendered_params_file` because `ParameterFile.cleanup()` deletes
    the temporary file. Callers must decide explicitly whether to store that path in the launch context with
    `SetLaunchConfiguration`.

    :param params_file: Path or URI (package://, file://, or regular path) to the ROS parameter YAML file.
    :param rendered_params_file: Filesystem path where the rendered YAML file will be written.
    :param ctx: Launch context used to evaluate substitutions in the parameter YAML file.
    :return: None.
    :raises NullFilePathError: If no params file path or URI is provided.
    :raises InvalidFileUriPatternError: If a URI does not follow one of the supported file URI formats.
    :raises PackageNotFoundError: If the package in a ``package://`` URI is not in the ROS package index.
    :raises FileNotFoundError: If the resolved params file path does not point to a file.
    :raises OSError: If the output directory cannot be created, or if the temporary or output file cannot be read
        or written.
    :raises UnicodeDecodeError: If the evaluated temporary YAML file is not valid UTF-8.
    :raises Exception: If ``launch_ros.parameter_descriptions.ParameterFile.evaluate`` fails while expanding
        substitutions.
    """
    params_file = Path(resolve_file(params_file))

    if not params_file.is_file():
        raise FileNotFoundError(f"Params file '{params_file}' does not exist.")

    output_path = Path(rendered_params_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    parameter_file = ParameterFile(str(params_file), allow_substs=True)

    try:
        evaluated_path = parameter_file.evaluate(ctx)
        output_path.write_text(Path(evaluated_path).read_text(encoding='utf-8'), encoding='utf-8')
    finally:
        parameter_file.cleanup()


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

        return float(value)

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
    :return: Tuple ``(resolved_path, data)`` where ``data`` is the parsed YAML object; it can be ``None``
        when the file contains only comments/whitespace.
    :raises NullFilePathError: If no YAML file path or URI is provided.
    :raises InvalidFileUriPatternError: If a URI does not follow one of the supported file URI formats.
    :raises PackageNotFoundError: If the package in a ``package://`` URI is not in the ROS package index.
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

    This function performs the replacement on the namespace string itself. It does not remove a leading
    '/' or a trailing '/'. For example, '/' becomes `new_sep`, and '/ns1/ns2/' becomes
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
    :raises InvalidFileUriPatternError: If a URI does not follow one of the supported file URI formats.
    :raises PackageNotFoundError: If the package in a ``package://`` URI is not in the ROS package index.
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

        return os.path.join(parent_path, relative_file)

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


# def set_robot_prefix(ctx: LaunchContext, robot_name_key: str = 'robot_name') -> list[LaunchDescriptionEntity]:
#     """
#     Set the 'robot_prefix' LaunchConfiguration by creating it from 'robot_name'.
#     :param ctx: Launch context.
#     :param robot_name_key: Key for the robot name LaunchConfiguration (default: 'robot_name').
#     :return: List with a SetLaunchConfiguration for 'robot_prefix'.
#     """
#     robot_name = LaunchConfiguration(robot_name_key).perform(ctx)
#     return [SetLaunchConfiguration('robot_prefix', create_robot_prefix(robot_name))]


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
