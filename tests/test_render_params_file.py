"""
Test ROS parameter file rendering.

These tests verify that ros2_launch_helpers expands launch substitutions inside parameter YAML files
and writes the rendered YAML to the requested output path.
"""

from launch import LaunchContext

import ros2_launch_helpers as rlh


def test_render_params_file_expands_launch_substitutions(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    output_path = tmp_path.joinpath('rendered.yaml')

    source_path.write_text(
        '/**/node:\n'
        '  ros__parameters:\n'
        '    frame_id: $(var robot_prefix)base_link\n',
        encoding='utf-8',
    )

    ctx = LaunchContext()
    ctx.launch_configurations['robot_prefix'] = 'robot_1_'

    rendered_params_file = rlh.render_params_file(source_path, output_path, ctx)

    assert output_path.read_text(encoding='utf-8') == (
        '/**/node:\n'
        '  ros__parameters:\n'
        '    frame_id: robot_1_base_link\n'
    )

    assert rendered_params_file == output_path
