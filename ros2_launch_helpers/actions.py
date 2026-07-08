"""
Launch actions for common ROS 2 launch configuration steps.

The actions in this module are intentionally thin. They read values from the launch context, call
helper functions, and write the computed values back into the launch context.
"""

from pathlib import Path

from launch import Action, LaunchContext
from launch.substitutions import LaunchConfiguration
from launch.utilities import perform_substitutions
from launch.utilities.type_utils import (
    SomeSubstitutionsType,
    normalize_to_list_of_substitutions,
    normalize_typed_substitution,
    perform_typed_substitution,
)
from .helpers import compute_global_namespace, compute_robot_namespace, compute_robot_prefix, render_params_file


def _as_launch_configuration(value: str | LaunchConfiguration) -> LaunchConfiguration:
    """
    If value is a string, return a LaunchConfiguration created from that string.

    If value is already a LaunchConfiguration, return it unchanged. Reject any other input type.

    Actions in this package intentionally accept only two input forms for values read from the
    launch context: a string key or an already-created LaunchConfiguration. A string key is
    converted to LaunchConfiguration, and an existing LaunchConfiguration is returned unchanged.
    More generic substitutions are rejected so callers do not pass expressions that hide which
    launch configuration key is being read.
    """
    if isinstance(value, str):
        return LaunchConfiguration(value)

    if isinstance(value, LaunchConfiguration):
        return value

    raise TypeError('Action inputs must be a launch configuration name string or LaunchConfiguration')


class ProcessParamsFile(Action):
    """
    Validate or render a resolved ROS params file and store the result in a launch configuration.
    """

    def __init__(
        self,
        params_file: SomeSubstitutionsType,
        allow_substs: bool | SomeSubstitutionsType,
        output_params_file_key: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.params_file = normalize_to_list_of_substitutions(params_file)
        self.allow_substs = normalize_typed_substitution(allow_substs, bool)
        self.output_params_file_key = output_params_file_key

    def execute(self, context: LaunchContext):
        params_file = perform_substitutions(context, self.params_file)
        allow_substs = perform_typed_substitution(context, self.allow_substs, bool)

        params_file_to_return = params_file

        if not Path(params_file_to_return).is_file():
            raise FileNotFoundError(f"Params file '{params_file_to_return}' does not exist.")

        if allow_substs:
            params_file_to_return = render_params_file(params_file_to_return, context)

        context.launch_configurations[self.output_params_file_key] = params_file_to_return


class SetGlobalNamespace(Action):
    """
    Resolve one namespace launch configuration as an absolute namespace.
    """

    def __init__(
        self, namespace: str | LaunchConfiguration = 'namespace', output_namespace_key: str = 'namespace', **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = _as_launch_configuration(namespace)
        self.output_namespace_key = output_namespace_key

    def execute(self, context: LaunchContext):
        namespace = self.namespace.perform(context)
        context.launch_configurations[self.output_namespace_key] = compute_global_namespace(namespace)


class SetRobotNamespace(Action):
    """
    Resolve a robot name inside a parent namespace and store the result in the launch context.
    """

    def __init__(
        self,
        namespace: str | LaunchConfiguration = 'namespace',
        robot_name: str | LaunchConfiguration = 'robot_name',
        robot_namespace_key: str = 'robot_namespace',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = _as_launch_configuration(namespace)
        self.robot_name = _as_launch_configuration(robot_name)
        self.robot_namespace_key = robot_namespace_key

    def execute(self, context: LaunchContext):
        namespace = self.namespace.perform(context)
        robot_name = self.robot_name.perform(context)

        context.launch_configurations[self.robot_namespace_key] = compute_robot_namespace(namespace, robot_name)


class SetRobotPrefix(Action):
    """
    Convert a robot name launch configuration into a robot prefix launch configuration.
    """

    def __init__(
        self, robot_name: str | LaunchConfiguration = 'robot_name', robot_prefix_key: str = 'robot_prefix', **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.robot_name = _as_launch_configuration(robot_name)
        self.robot_prefix_key = robot_prefix_key

    def execute(self, context: LaunchContext):
        robot_name = self.robot_name.perform(context)
        context.launch_configurations[self.robot_prefix_key] = compute_robot_prefix(robot_name)
