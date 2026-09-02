"""
Test ROS parameter file rendering.

These tests verify that ros2_launch_helpers expands launch substitutions inside parameter YAML files
and writes the rendered YAML to the requested output path.
"""

import pytest
from launch import LaunchContext

import ros2_launch_helpers as rlh


def test_render_params_file_renders_substitutions_to_output_path(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    output_path = tmp_path.joinpath('rendered.yaml')

    source_path.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n', encoding='utf-8'
    )

    ctx = LaunchContext()
    ctx.launch_configurations['robot_prefix'] = 'robot_1_'

    result = rlh.render_params_file(source_path, ctx, output_path)

    assert result is None
    assert output_path.read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: robot_1_base_link\n'
    )


def test_render_params_file_uses_the_requested_output_path(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    first_output_path = tmp_path.joinpath('first.yaml')
    second_output_path = tmp_path.joinpath('second.yaml')

    source_path.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n', encoding='utf-8'
    )

    first_ctx = LaunchContext()
    first_ctx.launch_configurations['robot_prefix'] = 'first_'

    second_ctx = LaunchContext()
    second_ctx.launch_configurations['robot_prefix'] = 'second_'

    rlh.render_params_file(source_path, first_ctx, first_output_path)
    rlh.render_params_file(source_path, second_ctx, second_output_path)

    assert first_output_path.read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: first_base_link\n'
    )
    assert second_output_path.read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: second_base_link\n'
    )


def test_render_params_file_accepts_resolved_path_string(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    output_path = tmp_path.joinpath('rendered.yaml')

    source_path.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n', encoding='utf-8'
    )

    ctx = LaunchContext()
    ctx.launch_configurations['robot_prefix'] = 'robot_1_'

    result = rlh.render_params_file(str(source_path), ctx, output_path)

    assert result is None
    assert output_path.read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: robot_1_base_link\n'
    )


def test_render_params_file_raises_file_not_found_for_missing_file(tmp_path):
    missing_path = tmp_path.joinpath('missing.yaml')

    with pytest.raises(FileNotFoundError, match='Params file'):
        rlh.render_params_file(missing_path, LaunchContext(), tmp_path.joinpath('rendered.yaml'))


def test_render_params_file_rejects_none_output_path(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    source_path.write_text('/**/node:\n  ros__parameters:\n    frame_id: base_link\n', encoding='utf-8')

    with pytest.raises(TypeError, match='output_path must be a str or Path-like value'):
        rlh.render_params_file(source_path, LaunchContext(), None)


def test_render_params_file_rejects_empty_output_path(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    source_path.write_text('/**/node:\n  ros__parameters:\n    frame_id: base_link\n', encoding='utf-8')

    with pytest.raises(ValueError, match='output_path must not be empty'):
        rlh.render_params_file(source_path, LaunchContext(), '')


def test_render_params_file_rejects_directory_output_path(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    source_path.write_text('/**/node:\n  ros__parameters:\n    frame_id: base_link\n', encoding='utf-8')

    with pytest.raises(IsADirectoryError, match='is a directory'):
        rlh.render_params_file(source_path, LaunchContext(), tmp_path)
