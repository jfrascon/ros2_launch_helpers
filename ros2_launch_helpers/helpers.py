import numbers
import os
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from ament_index_python.packages import get_package_share_directory
from benedict import benedict
from launch import LaunchContext, LaunchDescriptionEntity
from launch.actions import SetLaunchConfiguration
from launch.substitutions import LaunchConfiguration
from launch_ros.parameter_descriptions import ParameterFile

DEFAULT_LOG_OPTIONS = {
    'log-level': 'info',  # One of: 'debug', 'info', 'warn', 'error'
    'log-config-file': '',  # Path to a logging configuration file to use instead of the default
    'disable-stdout-logs': False,  # Whether to disable writing log messages to the console
    'disable-rosout-logs': False,  # Whether to disable writing log messages out to /rosout
    'disable-external-lib-logs': False,  # Whether to completely disable the use of an external logger
}


def default_log_options_str() -> str:
    return ','.join(f'{k}={v}' for k, v in DEFAULT_LOG_OPTIONS.items())


LOG_OPTIONS_DESC = (
    'key-value string, like "log-level=info,log-config-file=/path/to/loggers.conf,'
    'disable-stdout-logs=True,disable-rosout-logs=True,disable-external-lib-logs=True,logger1_name=<level>,'
    f'logger2_name=<level>". Optional (default: "{default_log_options_str()}")'
)

DEFAULT_NODE_OPTIONS = {
    'output': 'screen',  # One of: 'screen', 'log', 'both'
    'emulate_tty': True,  # Whether to emulate a TTY for the node's stdout/stderr (usually True for 'screen' or 'both')
    'respawn': False,  # Whether to respawn the node if it dies
    'respawn_delay': 0.0,  # Delay in seconds before respawning a node
}


def default_node_options_str() -> str:
    return ','.join(f'{k}={v}' for k, v in DEFAULT_NODE_OPTIONS.items())


NODE_OPTIONS_DESC = (
    'key-value string, like "output=both,emulate_tty=True,respawn=True,respawn_delay=3.0".'
    f'Optional (default: "{default_node_options_str()}")'
)

# ------------------------------------------------------------------------------
# Non-opaque functions.
# ------------------------------------------------------------------------------


def get_params(params_file: str, overlay_params_file_list: str = '') -> list[Any]:
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

    print(f'Parameters 1: {parameters[0]}')

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

    print(f'Parameters 2: {parameters}')

    return parameters


REMAPPINGS_DESC = 'key-value string, like "/a:=/b,/c:=d,e:=/f,g:=h"'


def parse_cli_remappings(cli_remappings: str = '') -> List[Tuple[str, str]]:
    """
    Parse the CLI remappings string into a list of (from, to) tuples.
    :param cli_remappings: Key-value string for topic remappings.
    :return: List of (from, to) tuples.

    Example
    cli_remappings="/a:=/b,/c:=d,e:=/f,g:=h"
    ouput: [('/a', '/b'), ('/c', 'd'), ('e', '/f'), ('g', 'h')]
    """
    remappings: List[Tuple[str, str]] = []

    cli_r = cli_remappings.strip()

    if not cli_r:
        return remappings

    for from_to in cli_r.split(','):
        from_to = from_to.strip()

        if not from_to:
            continue

        if ':=' not in from_to:
            continue  # Ignore invalid remapping

        from_expr, to_expr = from_to.split(':=', 1)

        from_expr = from_expr.strip()
        to_expr = to_expr.strip()

        if not from_expr or not to_expr:
            continue  # Ignore invalid remapping

        remappings.append((from_expr, to_expr))

    return remappings


def parse_cli_log_opts(cli_log_opts: str = '') -> List[str]:
    """
    Parse the CLI log options string into a ROS arguments string.
    :param cli_log_opts: Key-value string for ROS logging options.
    :return: ROS arguments string.

    Example
    cli_log_opts="log-level=info,log-config-file=/path/to/loggers.conf,disable-stdout-logs=True,
                  disable-rosout-logs=True,disable-external-lib-logs=True,logger1_name=<level>,logger2_name=<level>"
    output: "--ros-args --log-level info --log-config-file /path/to/loggers.conf --disable-stdout-logs
             --disable-rosout-logs --disable-external-lib-logs --logger logger1_name=<level>
             --logger logger2_name=<level>"
    """

    def to_ros_args(log_opts: Dict[str, Any]) -> List[str]:
        args = []

        for k, v in log_opts.items():
            if k == 'log-level' or k == 'log-config-file':
                v = v.strip()
                if v:
                    args.extend([f'--{k}', v])
            elif k == 'disable-stdout-logs' or k == 'disable-rosout-logs' or k == 'disable-external-lib-logs':
                if v:
                    args.append(f'--{k}')
            else:
                # Custom logger levels
                v = v.strip()
                if v:
                    args.extend([f'--logger {k}', v])

        return args

    # Passing default values to 'log_opts', so:
    # - In case a key is missing, it gets the default value.
    # - In case 'cli_log_opts' is empty, we use the default log options.
    log_opts: Dict[str, Union[str, bool]] = DEFAULT_LOG_OPTIONS.copy()

    cli_l_o = cli_log_opts.strip()

    if not cli_l_o:
        return to_ros_args(log_opts)

    for log_opt in cli_l_o.split(','):
        log_opt = log_opt.strip()

        if not log_opt or '=' not in log_opt:
            continue

        key, val = log_opt.split('=', 1)

        key = key.strip().lower()
        val = val.strip().lower()

        if not key or not val:
            continue

        match key:
            case 'log-level':
                if val in ('debug', 'info', 'warn', 'error'):
                    log_opts[key] = val
            case 'log-config-file':
                log_opts[key] = val  # No validation
            case 'disable-stdout-logs':
                if val in ('true', 'false'):
                    log_opts[key] = val == 'true'
            case 'disable-rosout-logs':
                if val in ('true', 'false'):
                    log_opts[key] = val == 'true'
            case 'disable-external-lib-logs':
                if val in ('true', 'false'):
                    log_opts[key] = val == 'true'
            case _:  # Should never happen
                # Custom logger levels
                log_opts[key] = val

    # print(f'Log options dict: {log_opts}')
    # print(f'ROS args: {to_ros_args(log_opts)}')

    return to_ros_args(log_opts)


def parse_cli_node_opts(cli_node_opts: str) -> Dict[str, Union[str, bool, float]]:
    """
    Parse the CLI node options string into a dictionary.
    :param cli_node_opts: Key-value string for node options.
    :return: Dictionary with node options.

    Example
    cli_node_opts="output=both,emulate_tty=True,respawn=True,respawn_delay=3.0"
    output: {'output': 'both', 'emulate_tty': True, 'respawn': True, 'respawn_delay': 3.0}
    """
    node_opts: Dict[str, Union[str, bool, float]] = DEFAULT_NODE_OPTIONS.copy()

    cli_n_o = cli_node_opts.strip()

    if not cli_n_o:
        return node_opts

    for node_opt in cli_n_o.split(','):
        node_opt = node_opt.strip()

        if not node_opt or '=' not in node_opt:
            continue

        key, val = node_opt.split('=', 1)

        key = key.strip().lower()
        val = val.strip().lower()

        if not key or key not in DEFAULT_NODE_OPTIONS or not val:
            continue

        match key:
            case 'output':
                if val in ('screen', 'log', 'both'):
                    node_opts[key] = val
            case 'emulate_tty':
                if val in ('true', 'false'):
                    node_opts[key] = val == 'true'
            case 'respawn':
                if val in ('true', 'false'):
                    node_opts[key] = val == 'true'
            case 'respawn_delay':
                try:
                    node_opts[key] = float(val)
                except Exception as e:
                    raise ValueError(f"Invalid value for 'respawn_delay': '{val}'") from e
            case _:  # Should never happen
                pass

    return node_opts


def validate_name(s: str) -> Optional[bool]:
    """
    Validate characters of a string segment.

    Returns:
        - True  -> all characters are valid (ASCII alnum or underscore).
        - False -> at least one invalid character found.
        - None  -> input is empty; considered 'not evaluable' at this level.

    Notes:
        - This function does not raise; it only reports.
        - Policy decisions (e.g., whether empty is allowed) belong to the caller.
    """
    if not s:
        return None

    # Check all characters.
    # Valid characters are ASCII alphanumeric or underscore.
    # If any other character is found, return False.
    return all((c == '_') or (c.isascii() and c.isalnum()) for c in s)


def validate_namespace(ns: str) -> None:
    """
    Validate a namespace string.

    Rules:
    - '' and '/' are permitted as special cases (root/empty).
    - Reject empty segments (no '//' allowed after trimming one leading and one trailing slash).
    - Each non-empty segment must pass validate_string(...)=True.
    """
    if ns in ('', '/'):
        return

    # In order to check if there are two or more '/' in a row, we need to first remove the leading and trailing slashes
    # if present.

    # Remove exactly one leading slash.
    if ns.startswith('/'):
        ns = ns[1:]

    # Remove exactly one trailing slash.
    if ns.endswith('/'):
        ns = ns[:-1]

    parts = ns.split('/') if ns else []

    for seg in parts:
        # Empty segments are not allowed by policy here.
        if seg == '':
            raise ValueError("Consecutive '/' are not allowed in a namespace")

        seg_validity = validate_name(seg)

        if seg_validity is False:
            raise ValueError('Namespace segments must be ASCII [A-Za-z0-9_] only')

        # if argument pass to validate_name is None or empty string, then None is returned, as a modd to say,
        # 'it is not valid, it is not invalid, it was not validated'.

        # seg_validity cannot be None here because seg != '' by guard above, but we keep a defensive check , just
        # in case of future changes.
        if seg_validity is None:
            raise ValueError('Internal: unexpected empty segment during validation')


# ------------------------------------------------------------------------------
# Opaque functions.
# ------------------------------------------------------------------------------


def set_global_namespace(ctx: LaunchContext, namespace_key: str = 'namespace') -> list[LaunchDescriptionEntity]:
    """
    Normalize and validate LaunchConfiguration(namespace_key) to a global form.

    Semantics:
    - ''  -> '/'
    - '/' -> '/'
    - 'ns', '/ns', 'ns/', '/ns/' -> '/ns'
    - Reject spaces, consecutive '/', and non [A-Za-z0-9_] chars in segments.

    Returns a list with a SetLaunchConfiguration when it must update the value;
    otherwise returns an empty list if no change is required.
    """
    ns = LaunchConfiguration(namespace_key).perform(ctx)

    # Empty becomes root '/'
    if ns == '':
        return [SetLaunchConfiguration(namespace_key, '/')]

    # Already root
    if ns == '/':
        return []

    # A namespace should not end in a '/'; it is not an error if the namespace ends with a '/', but it is convenient to
    # to remove if it is present '/'.
    ns_norm = ns[:-1] if ns.endswith('/') else ns

    # A global namespace must start with a '/'.
    if not ns_norm.startswith('/'):
        ns_norm = '/' + ns_norm

    validate_namespace(ns_norm)

    return [SetLaunchConfiguration(namespace_key, ns_norm)]


def set_robot_namespace(
    ctx: LaunchContext, namespace_key: str = 'namespace', robot_name_key: str = 'robot_name'
) -> list[LaunchDescriptionEntity]:
    """
    Sets the action SetLaunchConfiguration('robot_namespace', <value>), where the key 'robot_namespace' is the
    concatenation of namespace + '/' + robot_name.

    Examples
        return LaunchDescription(
            [
                ...
                set_robot_namespace(ctx, kwargs={'namespace_key': 'ns', 'robot_name_key': 'rn'}),
                LogInfo(msg=['Robot namespace: ', LaunchConfiguration('robot_namespace')])
                ...
            ]
        )
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

    namespace = LaunchConfiguration(namespace_key).perform(ctx)
    robot_name = LaunchConfiguration(robot_name_key).perform(ctx)

    # No need to check if robot_name is None, since 'perform' can't return None.

    # Remove extra white spaces at the beginning and the end of the robot's name.
    robot_name = robot_name.strip()

    if not robot_name:
        raise RuntimeError(f"'{robot_name_key}' must be a non-empty string")

    # robot_name is a non-empty string here, ok!.

    if namespace in ('', '/'):
        return [SetLaunchConfiguration('robot_namespace', namespace + robot_name)]

    # A namespace should not end in a '/', it is not an error if the namespace ends with a '/', but it is convenient to
    # to remove if it is present '/'.
    namespace = namespace[:-1] if namespace.endswith('/') else namespace

    validate_namespace(namespace)

    # If the execution flow gets here, then the namespace is valid too.
    # Note: The namespace does not end in '/', but it can or cannot start with '/', since this is a point we do not
    # enforce here.

    return [SetLaunchConfiguration('robot_namespace', namespace + '/' + robot_name)]


def set_robot_prefix(ctx: LaunchContext, robot_name_key: str = 'robot_name') -> list[LaunchDescriptionEntity]:
    robot_name = LaunchConfiguration(robot_name_key).perform(ctx)

    # No need to check if robot_name is None, since 'perform' can't return None.

    # Remove extra white spaces at the beginning and the end of the robot's name.
    robot_name = robot_name.strip()

    if not robot_name:
        raise RuntimeError(f"'{robot_name_key}' must be a non-empty string")

    # robot_name is a non-empty string here, ok!.

    return [SetLaunchConfiguration('robot_prefix', f'{robot_name}_')]


# @dataclass
# class HelpContext:
#     parameters: SomeParameters
#     remappings: List[Tuple[str, str]]
#     node_opts: Dict[str, Union[str, bool, float]]
#     ros_arguments: str


#     node_options_desc = (
#         'key-value string, like "output=both,emulate_tty=True,respawn=True,respawn_delay=3.0".'
#         f'Optional (default: "{_default_node_options_str()}")'
#     )


# def declare_std_launch_arguments(default_node_name: str = '') -> List[LaunchDescriptionEntity]:
#     """
#     Declare standard launch arguments for a ROS2 node.
#     :param default_node_name: Default name for the node (optional, default: '').
#     :return: List of launch description entities.

#     The standard launch arguments are:
#     - use_sim_time: Whether to use simulation time (default: False).
#     - namespace: ROS namespace (optional, default: '').
#     - node_name: Name for the node (default: provided by the caller, or '').
#     - params_file: Base YAML file with ros__parameters (required).
#     - overlay_params_file_list: Comma-separated list of YAML overlay files (optional, default: '').
#     - remappings: Key-value string for topic remappings (optional, default: '').
#     - log_options: Key-value string for ROS logging options (optional, default: see below).
#     - node_options: Key-value string for node options (optional, default: see below).
#     """
#     remappings_desc = (
#         'key-value string, like "/a:=/b,/c:=d,e:=/f,g:=h". Remappings passed by CLI have highest'
#         'precedence. Optional (default: "")'
#     )
#     # log-level: The default log level for the node. One of: debug, info, warn, error.
#     # log-config-file: Path to a logging configuration file to use instead of the default.
#     # disable-stdout-logs: Whether to disable writing log messages to the console.
#     # disable-rosout-logs: Whether to disable writing log messages out to /rosout. This can significantly save on
#     #                      network bandwidth, but external observers will not be able to monitor logging.
#     # disable-external-lib-logs: Whether to completely disable the use of an external logger. This may be faster in
#        some cases, but means that logs will not be written to disk
#     log_options_desc = (
#         'key-value string, like "log-level=info,log-config-file=/path/to/loggers.conf,'
#         'disable-stdout-logs=True,disable-rosout-logs=True,disable-external-lib-logs=True,logger1_name=<level>,'
#         f'logger2_name=<level>". Optional (default: "{_default_log_options_str()}")'
#     )

#     node_options_desc = (
#         'key-value string, like "output=both,emulate_tty=True,respawn=True,respawn_delay=3.0".'
#         f'Optional (default: "{_default_node_options_str()}")'
#     )

#     return [
#         DeclareLaunchArgument(
#             'use_sim_time',
#             default_value='False',
#             choices=['True', 'true', 'False', 'false'],
#             description='Use simulation clock if true',
#         ),
#         DeclareLaunchArgument('namespace', default_value='', description='ROS namespace (optional, default: "")'),
#         DeclareLaunchArgument('node_name', default_value=default_node_name, description='Name for the node'),
#         DeclareLaunchArgument('params_file', description='Base YAML with ros__parameters'),
#         DeclareLaunchArgument(
#             'overlay_params_file_list',
#             default_value='',
#             description='Comma-separated list of YAML overlays (applied in order; last wins).',
#         ),
#         # Order of precedence for remappings (highest to lowest): CLI remappings > ros_args -r > capsule remappings.
#         DeclareLaunchArgument('remappings', default_value='', description=remappings_desc),
#         DeclareLaunchArgument('log_options', default_value=DEFAULT_LOG_OPTIONS, description=log_options_desc),
#         # Order of precedence for node_options (highest to lowest): CLI node_options > capsule node_options.
#         DeclareLaunchArgument('node_options', default_value=DEFAULT_NODE_OPTIONS, description=node_options_desc),
#     ]


# def get_help_context(context: LaunchContext) -> HelpContext:
#     return HelpContext(
#         parameters=_get_params(
#             LaunchConfiguration('params_file').perform(context),
#             LaunchConfiguration('overlay_params_file_list').perform(context),
#         ),
#         remappings=_parse_cli_remappings(LaunchConfiguration('remappings').perform(context)),
#         node_opts=_parse_cli_node_opts(LaunchConfiguration('node_options').perform(context)),
#         ros_arguments=_parse_cli_log_opts(LaunchConfiguration('log_options').perform(context)),
#     )


# def pretty_print(help_context: HelpContext):
#     print('Parameters:')
#     for p in help_context.parameters:
#         print(f'  {p}')

#     print('Remappings:')
#     for f, t in help_context.remappings:
#         print(f'  {f} -> {t}')

#     print('Node options:')
#     for k, v in help_context.node_opts.items():
#         print(f'  {k}: {v}')

#     print('ROS arguments:')
#     print(f'  {help_context.ros_arguments}')
