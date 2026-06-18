"""
Test ROS parameter file rendering.

These tests verify that ros2_launch_helpers expands launch substitutions inside parameter YAML files
and writes the rendered YAML to the requested output path.
"""

from pathlib import Path

from launch import LaunchContext
from launch.actions import SetLaunchConfiguration

import ros2_launch_helpers as rlh


def test_process_params_file_renders_substitutions_and_updates_launch_context(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')

    source_path.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n', encoding='utf-8'
    )

    ctx = LaunchContext()
    ctx.launch_configurations['params_file'] = str(source_path)
    ctx.launch_configurations['params_file_allow_substs'] = 'True'
    ctx.launch_configurations['robot_namespace'] = '/robot_1'
    ctx.launch_configurations['robot_prefix'] = 'robot_1_'

    actions = rlh.process_params_file(ctx)

    assert len(actions) == 1
    assert isinstance(actions[0], SetLaunchConfiguration)

    actions[0].execute(ctx)
    rendered_path = ctx.launch_configurations['params_file']

    assert rendered_path != str(source_path)
    assert Path(rendered_path).read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: robot_1_base_link\n'
    )


def test_process_params_file_resolves_without_rendering_when_substitutions_are_disabled(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')

    source_path.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n', encoding='utf-8'
    )

    ctx = LaunchContext()
    ctx.launch_configurations['params_file'] = str(source_path)
    ctx.launch_configurations['params_file_allow_substs'] = 'False'

    actions = rlh.process_params_file(ctx)

    assert len(actions) == 1
    assert isinstance(actions[0], SetLaunchConfiguration)

    actions[0].execute(ctx)

    assert ctx.launch_configurations['params_file'] == str(source_path)


def test_render_params_file_expands_launch_substitutions(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    output_path = tmp_path.joinpath('rendered.yaml')

    source_path.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n', encoding='utf-8'
    )

    ctx = LaunchContext()
    ctx.launch_configurations['robot_prefix'] = 'robot_1_'

    assert rlh.render_params_file(source_path, output_path, ctx) is None

    assert output_path.read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: robot_1_base_link\n'
    )
