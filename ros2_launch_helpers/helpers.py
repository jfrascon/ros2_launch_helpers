import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, List, Optional, Tuple, Union

import yaml
from ament_index_python.packages import get_package_share_directory
from launch import LaunchContext, LaunchDescriptionEntity
from launch.actions import LogInfo
from launch_ros.parameter_descriptions import ParameterFile


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
