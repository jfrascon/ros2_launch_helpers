"""
Launch actions for common ROS 2 launch configuration steps.

The actions in this module are intentionally thin. They resolve launch substitutions, call helper
functions, and write the computed values back into the launch context.
"""

from pathlib import Path
from tempfile import NamedTemporaryFile

from launch import Action
from launch import LaunchContext
from launch.actions import SetLaunchConfiguration
from launch.utilities import perform_substitutions
from launch.utilities.type_utils import normalize_to_list_of_substitutions
from launch.utilities.type_utils import SomeSubstitutionsType

from .helpers import make_namespace_absolute
from .helpers import make_robot_namespace
from .helpers import make_robot_prefix
from .helpers import render_params_file


def _resolve_context_key(
    context: LaunchContext, key: SomeSubstitutionsType, argument_name: str
) -> str:
    resolved_key = perform_substitutions(context, normalize_to_list_of_substitutions(key))

    if not resolved_key:
        raise ValueError(f'{argument_name} must resolve to a non-empty launch configuration key.')

    return resolved_key


class RequireDirectory(Action):
    """Require a launch substitution to resolve to an existing directory."""

    def __init__(self, path: SomeSubstitutionsType, **kwargs) -> None:
        super().__init__(**kwargs)
        self.path = normalize_to_list_of_substitutions(path)

    def execute(self, context: LaunchContext):
        resolved_path = perform_substitutions(context, self.path)

        if not resolved_path:
            raise ValueError('path must resolve to a non-empty filesystem path.')

        path = Path(resolved_path)

        if not path.is_dir():
            raise FileNotFoundError(
                f"Required directory '{path}' does not exist or is not a directory."
            )


class RequireFile(Action):
    """Require a launch substitution to resolve to an existing file."""

    def __init__(self, path: SomeSubstitutionsType, **kwargs) -> None:
        super().__init__(**kwargs)
        self.path = normalize_to_list_of_substitutions(path)

    def execute(self, context: LaunchContext):
        resolved_path = perform_substitutions(context, self.path)

        if not resolved_path:
            raise ValueError('path must resolve to a non-empty filesystem path.')

        path = Path(resolved_path)

        if not path.is_file():
            raise FileNotFoundError(f"Required file '{path}' does not exist or is not a file.")


class RenderParamsFile(Action):
    """Render a params file and store its new path in a launch configuration."""

    def __init__(
        self,
        params_file: SomeSubstitutionsType,
        output_context_key: SomeSubstitutionsType,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.params_file = normalize_to_list_of_substitutions(params_file)
        self.output_context_key = normalize_to_list_of_substitutions(output_context_key)

    def execute(self, context: LaunchContext):
        params_file = perform_substitutions(context, self.params_file)
        output_context_key = _resolve_context_key(
            context, self.output_context_key, 'output_context_key'
        )

        if not Path(params_file).is_file():
            raise FileNotFoundError(f"Params file '{params_file}' does not exist.")

        with NamedTemporaryFile(prefix='params_', suffix='.yaml', delete=False) as temp_file:
            output_path = Path(temp_file.name)

        render_params_file(params_file, context, output_path)
        context.launch_configurations[output_context_key] = str(output_path)


class ResolveParamsFile(Action):
    """
    Select a direct parameter file or render a template and publish its path in launch context.

    Exactly one of params_file and template_params_file must exist and be a regular file.
    Both paths and output_context_key accept launch substitutions. No filenames or directory
    layout are imposed: the caller supplies both candidate paths explicitly.

    The action returns one child action. Launch executes that child before continuing with
    subsequent actions: SetLaunchConfiguration publishes the direct path, while RenderParamsFile
    renders the template and publishes the generated path under the same output_context_key.
    Consumers should disable further substitution rendering when they use that selected path.
    """

    def __init__(
        self,
        params_file: SomeSubstitutionsType,
        template_params_file: SomeSubstitutionsType,
        output_context_key: SomeSubstitutionsType,
        **kwargs,
    ) -> None:
        """Store both candidate paths and the output key; resolve them when launch executes."""
        super().__init__(**kwargs)
        self.params_file = normalize_to_list_of_substitutions(params_file)
        self.template_params_file = normalize_to_list_of_substitutions(template_params_file)
        self.output_context_key = normalize_to_list_of_substitutions(output_context_key)

    def execute(self, context: LaunchContext) -> list[Action]:
        """Validate the candidates and return the action that prepares the selected file path."""
        # Resolve paths now so LaunchConfiguration values set by earlier actions are available.
        params_file = perform_substitutions(context, self.params_file)
        template_params_file = perform_substitutions(context, self.template_params_file)
        output_context_key = _resolve_context_key(
            context, self.output_context_key, 'output_context_key'
        )
        for name, value in (
            ('params_file', params_file), ('template_params_file', template_params_file)
        ):
            if not value:
                raise ValueError(f'{name} must resolve to a non-empty filesystem path.')

        # Check presence before checking file type. A directory must cause an error rather than
        # silently selecting the other candidate, and two existing paths are always ambiguous.
        direct_path = Path(params_file)
        template_path = Path(template_params_file)
        candidates = [path for path in (direct_path, template_path) if path.exists()]
        if not candidates:
            raise FileNotFoundError(
                f"Exactly one parameter file must exist: '{direct_path}' or '{template_path}'. "
                'Neither file exists.'
            )
        if len(candidates) != 1:
            raise ValueError(
                f"Exactly one parameter file must exist: '{direct_path}' or '{template_path}'. "
                'Both paths exist.'
            )
        selected_path = candidates[0]
        if not selected_path.is_file():
            raise ValueError(f"Parameter path '{selected_path}' must be a file.")

        # Return the child for launch to execute; do not render or modify the context here.
        # Only the template branch creates a new file. The direct branch retains its source path.
        if selected_path == template_path:
            return [
                RenderParamsFile(
                    params_file=str(selected_path), output_context_key=output_context_key
                )
            ]
        return [SetLaunchConfiguration(output_context_key, str(selected_path))]


class SetGlobalNamespace(Action):
    """Store one namespace as an absolute namespace in the launch context."""

    def __init__(
        self, namespace: SomeSubstitutionsType, output_context_key: SomeSubstitutionsType, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = normalize_to_list_of_substitutions(namespace)
        self.output_context_key = normalize_to_list_of_substitutions(output_context_key)

    def execute(self, context: LaunchContext):
        namespace = perform_substitutions(context, self.namespace)
        output_context_key = _resolve_context_key(
            context, self.output_context_key, 'output_context_key'
        )
        context.launch_configurations[output_context_key] = make_namespace_absolute(namespace)


class SetRobotNamespace(Action):
    """Store a robot name inside a parent namespace in the launch context."""

    def __init__(
        self,
        namespace: SomeSubstitutionsType,
        robot_name: SomeSubstitutionsType,
        output_context_key: SomeSubstitutionsType,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = normalize_to_list_of_substitutions(namespace)
        self.robot_name = normalize_to_list_of_substitutions(robot_name)
        self.output_context_key = normalize_to_list_of_substitutions(output_context_key)

    def execute(self, context: LaunchContext):
        namespace = perform_substitutions(context, self.namespace)
        robot_name = perform_substitutions(context, self.robot_name)
        output_context_key = _resolve_context_key(
            context, self.output_context_key, 'output_context_key'
        )

        context.launch_configurations[output_context_key] = make_robot_namespace(
            namespace, robot_name
        )


class SetRobotPrefix(Action):
    """Store a robot name as a robot prefix launch configuration."""

    def __init__(
        self,
        robot_name: SomeSubstitutionsType,
        output_context_key: SomeSubstitutionsType,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.robot_name = normalize_to_list_of_substitutions(robot_name)
        self.output_context_key = normalize_to_list_of_substitutions(output_context_key)

    def execute(self, context: LaunchContext):
        robot_name = perform_substitutions(context, self.robot_name)
        output_context_key = _resolve_context_key(
            context, self.output_context_key, 'output_context_key'
        )
        context.launch_configurations[output_context_key] = make_robot_prefix(robot_name)
