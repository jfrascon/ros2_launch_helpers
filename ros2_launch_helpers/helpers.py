import numbers
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import yaml
from ament_index_python.packages import get_package_share_directory
from benedict import benedict
from launch import LaunchContext, LaunchDescriptionEntity
from launch.actions import SetLaunchConfiguration
from launch.substitutions import LaunchConfiguration
from launch_ros.parameter_descriptions import ParameterFile

DEFAULT_LOGGING_OPTIONS = {
    'log-level': 'info',  # One of: 'debug', 'info', 'warn', 'error'
    'disable-stdout-logs': False,  # Whether to disable writing log messages to the console
    'disable-rosout-logs': False,  # Whether to disable writing log messages out to /rosout
    'disable-external-lib-logs': False,  # Whether to completely disable the use of an external logger
}


def default_logging_options_str(item_sep: str = ',', key_value_sep: str = '=') -> str:
    return item_sep.join(f'{k}{key_value_sep}{v}' for k, v in DEFAULT_LOGGING_OPTIONS.items())


LOGGING_OPTIONS_DESC = (
    'key-value string, like "log-level=info,disable-stdout-logs=True,disable-rosout-logs=True,'
    'disable-external-lib-logs=True,logger1_name=<level>,logger2_name=<level>".'
)

DEFAULT_NODE_OPTIONS = {
    'name': '',  # Node name (if empty, use default from the node executable)
    'output': 'screen',  # One of: 'screen', 'log', 'both'
    'emulate_tty': True,  # Whether to emulate a TTY for the node's stdout/stderr (usually True for 'screen' or 'both')
    'respawn': False,  # Whether to respawn the node if it dies
    'respawn_delay': 0.0,  # Delay in seconds before respawning a node
}


def default_node_options_str(item_sep: str = ',', key_value_sep: str = '=') -> str:
    return item_sep.join(f'{k}{key_value_sep}{v}' for k, v in DEFAULT_NODE_OPTIONS.items())


NODE_OPTIONS_DESC = (
    'key-value string, like "name=any_name,output=both,emulate_tty=True,respawn=True,respawn_delay=3.0".'
)

TOPIC_REMAPPINGS_DESC = 'key-value string, like "/a:=/b,/c:=d,e:=/f,g:=h"'

################################################################################
# Opaque functions.
################################################################################


def set_global_namespace(ctx: LaunchContext, namespace_key: str = 'namespace') -> list[LaunchDescriptionEntity]:
    """
    Set a global namespace LaunchConfiguration by normalizing the provided namespace.
    :param ctx: Launch context.
    :param namespace_key: Key for the namespace LaunchConfiguration (default: 'namespace').
    :return:
    Create a global namespace from the provided namespace.

    Semantics:
    - ''  -> '/'
    - '/' -> '/'
    - 'ns', '/ns', 'ns/', '/ns/' -> '/ns'
    - Reject spaces, consecutive '/', and non [A-Za-z0-9_] chars in segments.
    """
    namespace = LaunchConfiguration(namespace_key).perform(ctx)
    return [SetLaunchConfiguration(namespace_key, create_global_namespace(namespace))]


def set_robot_namespace(
    ctx: LaunchContext, namespace_key: str = 'namespace', robot_name_key: str = 'robot_name'
) -> list[LaunchDescriptionEntity]:
    """
    Set the 'robot_namespace' LaunchConfiguration by combining 'namespace' and 'robot_name'.
    :param ctx: Launch context.
    :param namespace_key: Key for the base namespace LaunchConfiguration (default: 'namespace').
    :param robot_name_key: Key for the robot name LaunchConfiguration (default: 'robot_name').
    :return: List with a SetLaunchConfiguration for 'robot_namespace'.
    """

    namespace = LaunchConfiguration(namespace_key).perform(ctx)
    robot_name = LaunchConfiguration(robot_name_key).perform(ctx)
    return [SetLaunchConfiguration('robot_namespace', create_robot_namespace(namespace, robot_name))]


def set_robot_prefix(ctx: LaunchContext, robot_name_key: str = 'robot_name') -> list[LaunchDescriptionEntity]:
    """
    Set the 'robot_prefix' LaunchConfiguration by creating it from 'robot_name'.
    :param ctx: Launch context.
    :param robot_name_key: Key for the robot name LaunchConfiguration (default: 'robot_name').
    :return: List with a SetLaunchConfiguration for 'robot_prefix'.
    """
    robot_name = LaunchConfiguration(robot_name_key).perform(ctx)
    return [SetLaunchConfiguration('robot_prefix', create_robot_prefix(robot_name))]


################################################################################
# Non-opaque functions.
################################################################################


def create_global_namespace(namespace: str) -> str:
    if not isinstance(namespace, str):
        raise ValueError('Namespace must be a string')

    namespace = namespace.strip()

    # Empty namespace becomes root '/'.
    if namespace == '':
        return '/'

    # Already root.
    if namespace == '/':
        return '/'

    # A namespace should not end in a '/'; remove any trailing '/' to normalize.
    namespace = namespace.rstrip('/')

    # A global namespace must start with a '/'.
    if not namespace.startswith('/'):
        namespace = '/' + namespace

    namespace_is_valid, error_msg = is_valid_namespace(namespace)

    if not namespace_is_valid:
        raise RuntimeError(f"Invalid namespace '{namespace}': {error_msg}")

    return namespace


def create_robot_namespace(namespace: str, robot_name: str) -> str:
    """
    Create a robot namespace by combining a base namespace and a robot name.
    :param namespace: Base namespace (can be empty or '/').
    :param robot_name: Robot name (must be a valid name).
    :return: Combined robot namespace.
    """
    # The 'robot_namespace' is the concatenation of the 'namespace' and the 'robot_name', using the character '/' a
    # separtor.
    # namespace=''          -> robot_namespace = robot_name
    # namespace='/'         -> robot_namespace = '/' + robot_name
    # namespace='ns'        -> robot_namespace = 'ns' + '/' + robot_name
    # namespace='ns/'       -> robot_namespace = 'ns' + '/' + robot_name
    # namespace='/ns/'      -> robot_namespace = '/ns' + '/' + robot_name
    # namespace='/ns1/ns2'  -> robot_namespace = '/ns1/ns2' + '/' + robot_name
    # namespace='/ns1/ns2/' -> robot_namespace = '/ns1/ns2' + '/' + robot_name

    if not isinstance(namespace, str):
        raise ValueError('Namespace must be a string')

    if not isinstance(robot_name, str):
        raise ValueError('Robot name must be a string')

    # Remove extra white spaces at the beginning and the end of the robot's name.
    robot_name = robot_name.strip()

    if not is_valid_name(robot_name):
        raise RuntimeError(f"'{robot_name}' must be a non-empty string with ASCII [A-Za-z0-9_] only")

    if namespace in ('', '/'):
        return namespace + robot_name

    # Ensure namespace does not end with '/', so we can safely concatenate 'namespace + '/' + robot_name', proven
    # the namespace is valid.
    if namespace.endswith('/'):
        namespace = namespace.rstrip('/')

    namespace_is_valid, error_msg = is_valid_namespace(namespace)

    if not namespace_is_valid:
        raise RuntimeError(f"Invalid namespace '{namespace}': {error_msg}")

    # Note: The namespace does not end in '/', but it may or may not start with '/', depending on what the user
    # provided, since this is not enforced here.

    return f'{namespace}/{robot_name}'


def create_robot_prefix(robot_name: str) -> str:
    if not isinstance(robot_name, str):
        raise ValueError('Robot name must be a string')

    # Remove extra white spaces at the beginning and the end of the robot's name.
    robot_name = robot_name.strip()

    if not is_valid_name(robot_name):
        raise RuntimeError(f"'{robot_name}' must be a non-empty string with ASCII [A-Za-z0-9_] only")

    # If the robot name already ends with '_', return it as is.
    # It is discouraged to have robot names ending with '_', but technically it is allowed, since
    # the 'is_valid_name' function allows it.
    if robot_name.endswith('_'):
        return robot_name
    else:
        return f'{robot_name}_'


def dottify_namespace(namespace: str) -> str:
    """
    Convert a namespace into a dot-separated format.
    :param namespace: Namespace string.
    :return: Dot-separated namespace string.
    """
    return _replace_separator_in_namespace(namespace, '.')


def underscorify_namespace(namespace: str) -> str:
    """
    Convert a namespace into an underscore-separated format.
    :param namespace: Namespace string.
    :return: Underscore-separated namespace string.
    """
    return _replace_separator_in_namespace(namespace, '_')


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
        - This function does not raise; it only reports.
        - Policy decisions (e.g., whether empty is allowed) belong to the caller.
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


def is_valid_namespace(ns: str) -> Tuple[bool, str]:
    """
    Validate a namespace string.

    Rules:
    - '' and '/' are permitted as special cases (root/empty).
    - Reject empty segments (no '//' allowed at any position).
    - Each non-empty segment must be valid, i.e., ASCII alnum or underscore only, [A-Za-z0-9_].
    """
    if ns in ('', '/'):
        return (True, '')

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
        return (False, "Namespace cannot be empty after removing leading and trailing '/'")

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
            return (False, "Consecutive '/' are not allowed in a namespace")

        # Check the item is in valid, which means ASCII alnum or underscore only, [A-Za-z0-9_].
        if not is_valid_name(item):
            return (False, 'Namespace segments must be ASCII [A-Za-z0-9_] only')

    return (True, '')


def merge_yaml_maps_strict(
    defaults: Mapping[str, Any],
    override: Mapping[str, Any],
    keypath_sep: str = '§',
    flat_sep: str = '.',
    allow_none: bool = True,
    numeric_compat: bool = False,
) -> Tuple[Dict[str, Any], List, List]:
    def same_type(a, b, numeric_compat: bool = False) -> bool:
        """
        Return True if 'b' is allowed to override 'a' according to type rules.

        Rules:
        1) Exact type match passes (type(a) is type(b)).
        2) If numeric_compat is True, allow int <-> float interchange,
            but never allow bool (since bool is a subclass of int in Python).
        3) Otherwise, types must match exactly.

        Examples:
        same_type(3, 7) -> True
        same_type(3, 7.0) -> False
        same_type(3, 7.0, numeric_compat=True) -> True
        same_type(True, 1, numeric_compat=True) -> False  # bool explicitly excluded
        same_type([1], [2]) -> True
        same_type([1], "x") -> False
        """
        if type(a) is type(b):
            return True

        # Optional numeric compatibility (int <-> float), but exclude bool explicitly.
        # 'numbers.Real' captures int and float and bool, so we must filter bool out.
        if numeric_compat and isinstance(a, numbers.Real) and isinstance(b, numbers.Real):
            return not isinstance(a, bool) and not isinstance(b, bool)

        # All other combinations are not allowed.
        return False

    # Wrap with benedict using a keypath separator that doesn't appear in keys
    d = benedict(defaults, keypath_separator=keypath_sep)
    o = benedict(override, keypath_separator=keypath_sep)

    # Flatten both with dotted paths (independent from keypath sep)
    d_flatten = d.flatten(flat_sep)
    o_flatten = o.flatten(flat_sep)

    merged_flat = {}
    applied = []
    ignored = []

    # Walk only default keys -> overlay strict
    for dk, dv in d_flatten.items():
        if dk in o_flatten:
            ov = o_flatten[dk]
            # Allow None values if specified
            if ov is None:
                if allow_none:
                    merged_flat[dk] = None
                    applied.append(dk)
                else:
                    # Treat None as 'missing override': inherit default and record as ignored
                    merged_flat[dk] = dv
                    ignored.append(dk)
                continue

            if not same_type(dv, ov, numeric_compat):
                raise TypeError(f'{dk}: type mismatch (default={type(dv).__name__}, override={type(ov).__name__})')

            # Lists replace lists; scalars replace scalars – both covered by type check
            merged_flat[dk] = ov
            applied.append(dk)
        else:
            merged_flat[dk] = dv

    # Collect extras from override (ignored by design)
    for ok in o_flatten.keys():
        if ok not in d_flatten:
            ignored.append(ok)

    # Rebuild nested dict
    nested = benedict(merged_flat, keypath_separator=keypath_sep).unflatten(separator=flat_sep)

    return nested, applied, ignored


def process_logging_options(
    logging_options_kvs: Optional[str], item_sep: str = ',', key_value_sep: str = '='
) -> List[str]:
    """
    Parse the log options string into a ROS arguments string.
    :param logging_options_kvs: Key-value string for ROS logging options.
    :return: ROS arguments string.

    Reference: https://docs.ros.org/en/rolling/Tutorials/Demos/Logging-and-logger-configuration.html

    Example
    logging_options_kvs="log-level=info,disable-stdout-logs=True,disable-rosout-logs=True,disable-external-lib-logs=True,
                  logger1_name=<level>,logger2_name=<level>"
    output: [--log-level, info, --disable-stdout-logs, --disable-rosout-logs, --disable-external-lib-logs, --logger,
             logger1_name=<level>, --logger, logger2_name=<level>]
    """

    def to_ros_args(logging_options: Dict[str, Any]) -> List[str]:
        args = []

        for k, v in logging_options.items():
            if k == 'log-level':
                v = v.strip()
                if v:
                    args.extend([f'--{k}', v])
            elif k == 'disable-stdout-logs' or k == 'disable-rosout-logs' or k == 'disable-external-lib-logs':
                if v:
                    args.append(f'--{k}')
            # If it is not one of the known keys, it must be a custom logger level.
            else:
                v = v.strip()
                if v:
                    args.extend(['--log-level', f'{k}:={v}'])  # Note the ':=' separator for custom loggers.

        return args

    # Passing default values to 'logging_options', so:
    # - In case a key is missing, it gets the default value.
    # - In case 'logging_options_kvs' is empty, we use the default log options.
    logging_options: Dict[str, Union[str, bool]] = DEFAULT_LOGGING_OPTIONS.copy()

    # If no log options are provided, return the default log options as ROS args.
    if not isinstance(logging_options_kvs, str):
        return to_ros_args(logging_options)

    logging_options_kvs = logging_options_kvs.strip()

    # If the logging options string is empty, return the default logging options as ROS args.
    if not logging_options_kvs:
        return to_ros_args(logging_options)

    # Iterate over each key-value pair in the log options string.
    for logging_option in logging_options_kvs.split(item_sep):
        logging_option = logging_option.strip()

        # If the element between the item separators is empty or does not contain the key-value separator, skip it.
        if not logging_option or key_value_sep not in logging_option:
            continue

        # Split only on the first occurrence of the key-value separator.
        key, val = logging_option.split(key_value_sep, 1)

        # Strip leading and trailing spaces.
        # Key must be passed as is (case-sensitive), but we allow values to be case-insensitive, so we convert them to
        # lower case for easier comparison.
        key = key.strip()
        val = val.strip().lower()

        # If no key or value is provided, skip it.
        if not key or not val:
            continue

        match key:
            case 'log-level':
                if val in ('debug', 'info', 'warn', 'error'):
                    logging_options[key] = val
            case 'disable-stdout-logs':
                if val in ('true', 'false'):
                    logging_options[key] = val == 'true'
            case 'disable-rosout-logs':
                if val in ('true', 'false'):
                    logging_options[key] = val == 'true'
            case 'disable-external-lib-logs':
                if val in ('true', 'false'):
                    logging_options[key] = val == 'true'
            # If it is not one of the known keys, it must be a custom logger level.
            # A custom logger has the form 'logger_name=<level>'.
            # Important, for a logger to apply correctly in the node, its name must be fully qualified with the node's
            # namespace
            case _:
                if val in ('debug', 'info', 'warn', 'error', 'fatal'):
                    # key is the logger name, val is the log level.
                    logging_options[key] = val

    # print(f'Log options dict: {logging_options}')
    # print(f'ROS args: {to_ros_args(logging_options)}')

    return to_ros_args(logging_options)


def process_node_options(
    node_options_kvs: Optional[str], item_sep: str = ',', key_value_sep: str = '='
) -> Dict[str, Union[str, bool, float]]:
    """
    Parse the node options string into a dictionary.
    :param node_options_kvs: Key-value string for node options.
    :return: Dictionary with node options.

    Example
    node_options_kvs="name=<any_name>,output=both,emulate_tty=True,respawn=True,respawn_delay=3.0"
    output: {'name': <any_name>, 'output': 'both', 'emulate_tty': True, 'respawn': True, 'respawn_delay': 3.0}
    """
    node_options: Dict[str, Union[str, bool, float]] = DEFAULT_NODE_OPTIONS.copy()

    # If node_options_kvs is not a string, return the default node options.
    if not isinstance(node_options_kvs, str):
        return node_options

    # If the node_options_kvs is empty, return the default node options.
    node_options_kvs = node_options_kvs.strip()

    if not node_options_kvs:
        return node_options

    # Iterate over each key-value pair in the node options string, processing only known keys.
    for node_option in node_options_kvs.split(item_sep):
        node_option = node_option.strip()

        # If the element between separators is empty or does not contain the key-value separator, skip it.
        if not node_option or key_value_sep not in node_option:
            continue

        key, val = node_option.split(key_value_sep, 1)

        # Remove leading and trailing spaces and convert to lower case for easier comparison.
        key = key.strip().lower()
        val = val.strip().lower()

        # If no key is provided or the key is not known, skip it.
        if not key or key not in DEFAULT_NODE_OPTIONS:
            continue

        # Process known keys.
        match key:
            case 'name':
                # If the key is 'name', accept any non-empty value that is not 'default'.
                # If 'name' is empty or 'default', the value associated with 'name' is empty, which means the node
                # will use its default name.
                if val and val != 'default':
                    node_options[key] = val
            case 'output':
                if val in ('screen', 'log', 'both'):
                    node_options[key] = val
            case 'emulate_tty':
                if val in ('true', 'false'):
                    node_options[key] = val == 'true'
            case 'respawn':
                if val in ('true', 'false'):
                    node_options[key] = val == 'true'
            case 'respawn_delay':
                try:
                    node_options[key] = float(val)
                except Exception as e:
                    raise ValueError(f"Invalid value for 'respawn_delay': '{val}'") from e
            case _:  # Should never happen
                pass

    return node_options


def process_topic_remappings(
    topic_remappings_kvs: Optional[str], item_sep: str = ',', topic_remapping_sep: str = ':='
) -> Optional[List[Tuple[str, str]]]:
    """
    Parse the topic remappings string into a list of (from, to) tuples.
    :param topic_remappings_kvs: Key-value string for topic remappings.
    :return: List of (from, to) tuples.

    Example
    topic_remappings_kvs="/a:=/b,/c:=d,e:=/f,g:=h"
    ouput: [('/a', '/b'), ('/c', 'd'), ('e', '/f'), ('g', 'h')]
    """

    # If topic_remappings_kvs is not a string, return None.
    if not isinstance(topic_remappings_kvs, str):
        return None

    topic_remappings_kvs = topic_remappings_kvs.strip()

    # If the topic_remappings_kvs string is empty, return None.
    if not topic_remappings_kvs:
        return None

    topic_remappings: List[Tuple[str, str]] = []

    for topic_remapping in topic_remappings_kvs.split(item_sep):
        topic_remapping = topic_remapping.strip()

        # If the element between commas is empty, skip it.
        if not topic_remapping:
            continue

        if topic_remapping_sep not in topic_remapping:
            continue  # Ignore invalid topic remapping

        original_topic, new_topic = topic_remapping.split(topic_remapping_sep, 1)

        original_topic = original_topic.strip()
        new_topic = new_topic.strip()

        # If the original or new topic is empty, skip it.
        if not original_topic or not new_topic:
            continue  # Ignore invalid remapping

        topic_remappings.append((original_topic, new_topic))

    return topic_remappings


def read_yaml_file(yaml_file: str) -> Tuple[str, Any]:
    """
    Read and parse a YAML file, returning both the resolved path and the loaded object.

    :param yaml_file: Path or URI (package://, file://, or regular path) to the YAML file.
    :return: Tuple ``(resolved_path, data)`` where ``data`` is the parsed YAML object; it can be ``None``
        when the file contains only comments/whitespace.
    :raises ValueError: If the input path is invalid, the file cannot be found/read, or YAML syntax is invalid.
    """
    if not isinstance(yaml_file, str):
        raise ValueError(f'YAML file must be a str (got: {type(yaml_file).__name__})')

    yaml_file = yaml_file.strip()

    if not yaml_file:
        raise ValueError('YAML file not provided')

    # Resolve URI formats (package://, file://) and expand '~' for regular filesystem paths.
    try:
        resolved_yaml_file = resolve_file(yaml_file)
    except Exception as e:
        raise ValueError(f"Failed to resolve YAML file '{yaml_file}': {e}") from e

    resolved_yaml_path = Path(resolved_yaml_file)

    if not resolved_yaml_path.is_file():
        raise ValueError(f"File '{resolved_yaml_file}' does not exist")

    try:
        with resolved_yaml_path.open('r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML syntax in file '{resolved_yaml_file}': {e}") from e
    except (OSError, UnicodeDecodeError) as e:
        raise ValueError(f"Failed to read file '{resolved_yaml_file}': {e}") from e
    except Exception as e:
        raise ValueError(f"Unexpected error reading YAML file '{resolved_yaml_file}': {e}") from e

    return (resolved_yaml_file, data)


def read_yaml_mapping(yaml_file: str) -> Tuple[str, Dict[str, Any]]:
    """
    Load a YAML file and ensure the top-level document is a mapping, returning its resolved path and data.

    :param yaml_file: Path or URI (package://, file://, or regular path) to the YAML file.
    :return: Tuple '(resolved_path, mapping)' where 'mapping' is the parsed YAML dict.
    :raises ValueError: If the file cannot be resolved/read, has invalid YAML, is empty, or the top-level
        object is not a mapping.
    """
    resolved_yaml_file, data = read_yaml_file(yaml_file)

    if data is None:
        raise ValueError(f"File '{resolved_yaml_file}' is empty; expected a mapping")

    if not isinstance(data, dict):
        raise ValueError(f"File '{resolved_yaml_file}' must be a mapping. Got: '{type(data).__name__}'")

    return resolved_yaml_file, data


def resolve_file(file: Optional[str]) -> str:
    if file is None:
        return ''

    if not isinstance(file, str):
        raise ValueError(f'File must be a string (got: {type(file).__name__})')

    file = file.strip()

    if not file:
        return ''

    if file.startswith('package://'):
        rest = file[len('package://') :]

        if '/' not in rest:
            raise ValueError(f"File URI must be 'package://<pkg>/<path>' (got: '{file}')")

        pkg, relative_file = rest.split('/', 1)
        # Raises 'PackageNotFoundError' if the package is not found.
        # Raises 'ValueError' if the package name is invalid.
        parent_path = get_package_share_directory(pkg)
        return os.path.join(parent_path, relative_file)

    if file.startswith('file://'):
        resolved_file = os.path.expanduser(file[len('file://') :])

        if not os.path.isabs(resolved_file):
            raise ValueError(f"File URI must point to an absolute path (got: '{resolved_file}')")

        return resolved_file

    # If none of the special URI formats matched, return the original string with user expansion, if
    # possible.
    return os.path.expanduser(file)


def _replace_separator_in_namespace(namespace: str, new_sep: str) -> str:
    if not isinstance(namespace, str):
        raise ValueError('Namespace must be a string')

    if not isinstance(new_sep, str) or len(new_sep) != 1:
        raise ValueError('New separator must be a single character string')

    # No conversion happens for these cases.
    # '' -> ''
    # '/' -> ''
    if namespace in ('', '/'):
        return ''

    namespace_is_valid, error_msg = is_valid_namespace(namespace)

    if not namespace_is_valid:
        raise RuntimeError(f"Invalid namespace '{namespace}': {error_msg}")

    # Now that we know the namespace is valid, we transform the '/' separators into '<new_sep>' separators.
    # Be aware that, leading and trailing '/' are removed in the process.

    # If we consider <c> as the new separator, for the sake of the examples:
    # namespace=''          -> ''
    # namespace='/'         -> ''
    # namespace='ns'        -> ns
    # namespace='ns/'       -> ns
    # namespace='/ns/'      -> ns
    # namespace='/ns1/ns2'  -> ns1<c>ns2
    # namespace='/ns1/ns2/' -> ns1<c>ns2

    return namespace.strip('/').replace('/', new_sep)
