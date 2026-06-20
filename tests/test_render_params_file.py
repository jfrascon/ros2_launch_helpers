"""
Test ROS parameter file rendering.

These tests verify that ros2_launch_helpers expands launch substitutions inside parameter YAML files
and writes the rendered YAML to the requested output path.
"""

from pathlib import Path

import pytest
from launch import LaunchContext

import ros2_launch_helpers as rlh


def test_render_params_file_renders_substitutions_to_temporary_file(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')

    source_path.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n', encoding='utf-8'
    )

    ctx = LaunchContext()
    ctx.launch_configurations['robot_prefix'] = 'robot_1_'

    rendered_path = rlh.render_params_file(source_path, ctx)

    assert rendered_path != str(source_path)
    assert Path(rendered_path).read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: robot_1_base_link\n'
    )


def test_render_params_file_uses_unique_rendered_paths(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')

    source_path.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n', encoding='utf-8'
    )

    first_ctx = LaunchContext()
    first_ctx.launch_configurations['robot_prefix'] = 'first_'

    second_ctx = LaunchContext()
    second_ctx.launch_configurations['robot_prefix'] = 'second_'

    first_rendered_path = Path(rlh.render_params_file(source_path, first_ctx))
    second_rendered_path = Path(rlh.render_params_file(source_path, second_ctx))

    assert first_rendered_path != second_rendered_path
    assert first_rendered_path.read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: first_base_link\n'
    )
    assert second_rendered_path.read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: second_base_link\n'
    )


def test_render_params_file_accepts_resolved_path_string(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')

    source_path.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n', encoding='utf-8'
    )

    ctx = LaunchContext()
    ctx.launch_configurations['robot_prefix'] = 'robot_1_'

    rendered_path = Path(rlh.render_params_file(str(source_path), ctx))

    assert rendered_path.read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: robot_1_base_link\n'
    )


def test_render_params_file_raises_file_not_found_for_missing_file(tmp_path):
    missing_path = tmp_path.joinpath('missing.yaml')

    with pytest.raises(FileNotFoundError, match='Params file'):
        rlh.render_params_file(missing_path, LaunchContext())
