import os
from pathlib import Path
from typing import Any, List, Literal, Optional, Tuple, Union

from ament_index_python.packages import get_package_share_directory
from launch import LaunchContext, LaunchDescriptionEntity
from launch.actions import LogInfo
from launch_ros.parameter_descriptions import ParameterFile
import rclpy.validate_namespace
import yaml


class FileResolutionError(ValueError):
    """
    Report a file value that cannot be converted into a filesystem path.

    Callers can catch this class when they do not need to distinguish between an empty value, an
    invalid URI shape, or another file-resolution error reported by this module.
    """


class NullFilePathError(FileResolutionError):
    """
    Report a missing file path or URI.

    This exception is raised when the caller passes ``None`` or an empty string.
    """


class InvalidFileUriPatternError(FileResolutionError):
    """
    Raised when a URI-like file value is not one of the formats accepted by ``resolve_file``.

    This includes malformed ``package://`` values, malformed ``file://`` values, and unsupported
    URI schemes such as ``http://``.
    """


def flatten_namespace(namespace: str, new_sep: str) -> str:
    """
    Convert a namespace into one flat string.

    ``namespace`` can be empty, relative, or absolute.
    The ``namespace`` is validated before flattening.
    The root namespace ``/`` and the empty namespace, ``''``, both return ``''`` because they do not
    contain a concrete namespace segment.
    ``new_sep`` must be one character other than ``/`` so the result no longer contains namespace
    separators.

    For example, ``/fleet/robot1`` with ``new_sep="_"`` returns ``fleet_robot1``.
    """
    validate_namespace(namespace)

    if not isinstance(new_sep, str) or len(new_sep) != 1 or new_sep == '/':
        raise ValueError("New separator must be a single character other than '/'")

    if namespace in ('', '/'):
        return ''

    return namespace.removeprefix('/').replace('/', new_sep)


def make_namespace_absolute(namespace: str) -> str:
    """
    Return an absolute namespace derived from the provided namespace.

    ``namespace`` is the namespace value provided by the user.
    It can be empty, relative, absolute, or ``/``.
    The root namespace ``/`` is returned unchanged.
    The empty namespace ``''`` and relative values become absolute by adding one leading ``/``.
    """
    validate_namespace(namespace)

    if namespace.startswith('/'):
        return namespace

    return f'/{namespace}'


def make_robot_namespace(namespace: str, robot_name: str) -> str:
    """
    Return the namespace for one robot inside a parent namespace.

    ``namespace`` is the parent namespace.
    ``robot_name`` is the name segment that identifies the robot.

    The result preserves whether the parent namespace is relative or absolute.
    An empty or relative parent namespace produces a relative robot namespace.
    A root or absolute parent namespace produces an absolute robot namespace.

    For example, ``fleet`` and ``robot1`` produce ``fleet/robot1``.
    If ``namespace`` is ``/fleet`` and ``robot_name`` is ``robot1``, the returned namespace is
    ``/fleet/robot1``.

    Use ``make_namespace_absolute`` on the result when the caller always needs an absolute robot
    namespace.
    """
    validate_namespace(namespace)
    validate_name_segment(robot_name)

    if namespace in ('', '/'):
        robot_namespace = namespace + robot_name
    else:
        robot_namespace = f'{namespace}/{robot_name}'

    validate_namespace(robot_namespace)
    return robot_namespace


def make_robot_prefix(robot_name: str) -> str:
    """
    Return the prefix derived from one robot name.

    This function is just a wrapper around ``to_prefix()`` with a more descriptive name for callers
    that want a prefix derived from a robot name.

    The prefix is used by callers that need a stable text prefix for frame names, topic names, or
    other identifiers.
    If ``robot_name`` already ends with ``_``, it is returned as-is.
    Otherwise, one trailing ``_`` is added.
    """
    return to_prefix(robot_name)


def read_yaml_file(yaml_file: Optional[Union[str, Path]]) -> Tuple[str, Any]:
    """
    Resolve, read, and parse one YAML file.

    ``yaml_file`` can be a regular path, a ``file://`` URI, or a ``package://`` URI accepted by
    ``resolve_file``.
    The returned tuple contains the resolved filesystem path and the Python value returned by
    ``yaml.safe_load``.

    If the YAML file only contains comments or whitespace, the parsed value is ``None``.
    The function raises if the path cannot be resolved, the resolved path is not a file, the file
    cannot be read as UTF-8, or the YAML content is invalid.
    """
    resolved_yaml_file = resolve_file(yaml_file)
    resolved_yaml_path = Path(resolved_yaml_file)

    if not resolved_yaml_path.is_file():
        raise FileNotFoundError(f"Path '{resolved_yaml_file}' does not point to a file")

    with resolved_yaml_path.open('r', encoding='utf-8') as f:
        data = yaml.safe_load(f)

    return (resolved_yaml_file, data)


def require_list(data: Any) -> None:
    """Require parsed data to be a list."""
    if not isinstance(data, list):
        raise TypeError(f"Expected a list. Got: '{type(data).__name__}'")


def require_mapping(data: Any) -> None:
    """Require parsed data to be a mapping."""
    if not isinstance(data, dict):
        raise TypeError(f"Expected a mapping. Got: '{type(data).__name__}'")


def require_non_empty_list(data: Any) -> None:
    """Require parsed data to be a non-empty list."""
    require_list(data)

    if not data:
        raise ValueError('Expected a non-empty list')


def require_non_empty_mapping(data: Any) -> None:
    """Require parsed data to be a non-empty mapping."""
    require_mapping(data)

    if not data:
        raise ValueError('Expected a non-empty mapping')


def render_params_file(
    params_file: Union[str, Path], ctx: LaunchContext, output_path: Union[str, Path]
) -> None:
    """
    Expand launch substitutions in one ROS parameter YAML file and write the rendered content.

    ``params_file`` must already be a filesystem path. This function does not resolve
    ``package://`` or ``file://`` values.

    ``output_path`` is the caller-owned filesystem path where the rendered YAML is written.
    It must be a non-empty path to a file.

    ``ParameterFile.evaluate()`` expands expressions such as ``$(var robot_name)`` using the
    provided launch context. The path returned by ``ParameterFile.evaluate()`` belongs to
    ``ParameterFile`` and is deleted when ``cleanup()`` runs, so this helper copies the rendered
    YAML into ``output_path`` before cleaning up.

    If this function finishes without raising an exception, the rendered YAML has been written
    to ``output_path``.
    """
    params_file = Path(params_file)

    if not params_file.is_file():
        raise FileNotFoundError(f"Params file '{params_file}' does not exist.")

    if output_path is None:
        raise TypeError('output_path must be a str or Path-like value, got None.')

    if isinstance(output_path, str) and not output_path:
        raise ValueError('output_path must not be empty.')

    output_path = Path(output_path)

    if output_path.is_dir():
        raise IsADirectoryError(f"Output path '{output_path}' is a directory.")

    parameter_file = ParameterFile(str(params_file), allow_substs=True)

    try:
        evaluated_path = parameter_file.evaluate(ctx)
        output_path.write_text(Path(evaluated_path).read_text(encoding='utf-8'), encoding='utf-8')
    finally:
        parameter_file.cleanup()


def replace_separator_in_namespace(namespace: str, new_sep: str) -> str:
    """
    Replace every ``/`` character in a valid namespace with another single character.

    ``namespace`` can be empty, relative, or absolute.
    It must pass ``validate_namespace``.
    ``new_sep`` must be a one-character string.
    Passing ``/`` leaves the namespace unchanged.

    The root marker is preserved.
    For example, ``/ns1/ns2`` with ``new_sep="_"`` returns ``_ns1_ns2``.
    Use ``flatten_namespace`` when the result must not include that marker.
    """
    validate_namespace(namespace)

    if not isinstance(new_sep, str) or len(new_sep) != 1:
        raise ValueError('New separator must be a single character string')

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
            raise InvalidFileUriPatternError(
                f"Package URI must be 'package://<package>/<path>' (got: '{file}')"
            )

        pkg, relative_file = rest.split('/', 1)

        if not pkg or not relative_file:
            raise InvalidFileUriPatternError(
                f"Package URI must be 'package://<package>/<path>' (got: '{file}')"
            )

        try:
            parent_path = get_package_share_directory(pkg)
        except ValueError as e:
            raise InvalidFileUriPatternError(
                f"Package URI contains an invalid package name (got: '{file}')"
            ) from e

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
            raise InvalidFileUriPatternError(
                f"File URI must point to an absolute path (got: '{resolved_file}')"
            )

        return resolved_file

    if '://' in file:
        scheme = file.split('://', 1)[0]
        raise InvalidFileUriPatternError(f"Unsupported file URI scheme '{scheme}' in '{file}'")

    # If none of the special URI formats matched, return the original string with user expansion, if
    # possible.
    return os.path.expanduser(file)


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


def to_prefix(name_segment: str) -> str:
    """
    Convert one valid name segment into a prefix string.

    ``name_segment`` must pass ``validate_name_segment``. If it already ends with ``_``, it is
    returned unchanged. Otherwise, the returned prefix is ``name_segment`` plus one trailing ``_``.

    This helper is used when a caller wants names such as ``robot`` to become stable prefixes such
    as ``robot_`` without adding a second underscore to names that already have one.
    """
    validate_name_segment(name_segment)

    if name_segment.endswith('_'):
        return name_segment

    return f'{name_segment}_'


def validate_name_segment(name_segment: str) -> Literal[True]:
    """
    Validate one project name segment and return ``True``.

    ROS 2 defines a "name token" on this design page:
    https://design.ros2.org/articles/topic_and_service_names.html#name-tokens

    A name token is one non-empty part between the ``/`` separators of a topic or service name.
    ROS 2 name tokens may contain ASCII letters, numbers, underscores, balanced substitutions in
    ``{}``, or the private-name token ``~``.
    A token must not start with a number.

    This project uses the separate term "name segment" because this function does not implement
    every ROS 2 name-token form.
    A name segment identifies a generic project entity, such as a robot or another named resource.
    It is not itself a topic, service, namespace, node name, or package name.

    A name segment must start with an ASCII letter.
    Every remaining character must be an ASCII letter, a number, or ``_``.
    This intentionally rejects substitutions, ``~``, separators, and a leading underscore.
    The stricter rules make the segment safe to include later in concrete ROS names.
    The complete topic, service, or namespace must still be validated with its matching ROS 2
    validator after it is constructed.
    """
    if not isinstance(name_segment, str):
        raise TypeError('name_segment must be a string')

    if not name_segment:
        raise ValueError('name_segment must not be empty')

    first_character = name_segment[0]

    if not first_character.isascii() or not first_character.isalpha():
        raise ValueError('name_segment must start with an ASCII letter')

    for index, character in enumerate(name_segment[1:], start=1):
        if character == '_' or (character.isascii() and character.isalnum()):
            continue

        raise ValueError(
            f'name_segment contains invalid character {character!r} at index {index}; '
            "only ASCII letters, numbers, and '_' are allowed"
        )

    return True


def validate_namespace(namespace: str) -> Literal[True]:
    """
    Validate one namespace value accepted by this launch helper module.

    The empty namespace ``''`` and the root namespace ``/`` are valid inputs.

    ``rclpy.validate_namespace.validate_namespace`` accepts only non-empty absolute namespaces.
    If a relative namespace is provided, this function converts it to an absolute namespace before
    calling the rclpy validator.
    The conversion is only used for validation; it does not modify the provided value.
    """
    if not isinstance(namespace, str):
        raise TypeError('namespace must be a string')

    if namespace in ('', '/'):
        return True

    absolute_namespace = namespace if namespace.startswith('/') else f'/{namespace}'
    rclpy.validate_namespace.validate_namespace(absolute_namespace)

    return True
