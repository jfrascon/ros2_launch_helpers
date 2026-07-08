"""
Launch actions for common ROS 2 launch configuration steps.

The actions in this module are intentionally thin. They resolve launch substitutions, call helper
functions, and write the computed values back into the launch context.
"""

from pathlib import Path

from launch import Action, LaunchContext
from launch.utilities import perform_substitutions
from launch.utilities.type_utils import (
    SomeSubstitutionsType,
    normalize_to_list_of_substitutions,
    normalize_typed_substitution,
    perform_typed_substitution,
)

from .helpers import compute_global_namespace, compute_robot_namespace, compute_robot_prefix, render_params_file


def _resolve_output_context_key(context: LaunchContext, key: SomeSubstitutionsType, argument_name: str) -> str:
    resolved_key = perform_substitutions(context, key)

    if not resolved_key:
        raise ValueError(f'{argument_name} must resolve to a non-empty launch configuration key.')

    return resolved_key


class ProcessParamsFile(Action):
    """
    Validate or render a resolved ROS params file and store the result in a launch configuration.
    """

    def __init__(
        self,
        params_file: SomeSubstitutionsType,
        allow_substs: bool | SomeSubstitutionsType,
        output_context_key: SomeSubstitutionsType,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.params_file = normalize_to_list_of_substitutions(params_file)
        self.allow_substs = normalize_typed_substitution(allow_substs, bool)
        self.output_context_key = normalize_to_list_of_substitutions(output_context_key)

    def execute(self, context: LaunchContext):
        params_file = perform_substitutions(context, self.params_file)
        allow_substs = perform_typed_substitution(context, self.allow_substs, bool)
        output_context_key = _resolve_output_context_key(context, self.output_context_key, 'output_context_key')

        params_file_to_return = params_file

        if not Path(params_file_to_return).is_file():
            raise FileNotFoundError(f"Params file '{params_file_to_return}' does not exist.")

        if allow_substs:
            params_file_to_return = render_params_file(params_file_to_return, context)

        context.launch_configurations[output_context_key] = params_file_to_return


class SetGlobalNamespace(Action):
    """
    Resolve one namespace value as an absolute namespace and store it in the launch context.
    """

    def __init__(self, namespace: SomeSubstitutionsType, output_context_key: SomeSubstitutionsType, **kwargs) -> None:
        super().__init__(**kwargs)
        self.namespace = normalize_to_list_of_substitutions(namespace)
        self.output_context_key = normalize_to_list_of_substitutions(output_context_key)

    def execute(self, context: LaunchContext):
        namespace = perform_substitutions(context, self.namespace)
        output_context_key = _resolve_output_context_key(context, self.output_context_key, 'output_context_key')
        context.launch_configurations[output_context_key] = compute_global_namespace(namespace)


class SetRobotNamespace(Action):
    """
    Resolve a robot name inside a parent namespace and store the result in the launch context.
    """

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
        output_context_key = _resolve_output_context_key(context, self.output_context_key, 'output_context_key')

        context.launch_configurations[output_context_key] = compute_robot_namespace(namespace, robot_name)


class SetRobotPrefix(Action):
    """
    Convert a robot name value into a robot prefix launch configuration.
    """

    def __init__(self, robot_name: SomeSubstitutionsType, output_context_key: SomeSubstitutionsType, **kwargs) -> None:
        super().__init__(**kwargs)
        self.robot_name = normalize_to_list_of_substitutions(robot_name)
        self.output_context_key = normalize_to_list_of_substitutions(output_context_key)

    def execute(self, context: LaunchContext):
        robot_name = perform_substitutions(context, self.robot_name)
        output_context_key = _resolve_output_context_key(context, self.output_context_key, 'output_context_key')
        context.launch_configurations[output_context_key] = compute_robot_prefix(robot_name)
