"""Check parameter source selection and the child actions returned to ROS launch."""

from pathlib import Path

from launch import LaunchContext
from launch.actions import SetLaunchConfiguration
from launch.conditions import IfCondition
from launch.substitutions import LaunchConfiguration
from launch.substitutions import PathJoinSubstitution
import pytest

import ros2_launch_helpers as rlh


@pytest.mark.parametrize('render_template', [False, True])
def test_resolve_params_file_selects_and_prepares_one_source(tmp_path, render_template):
    direct_file = tmp_path.joinpath('direct', 'settings.yaml')
    template_file = tmp_path.joinpath('templates', 'settings.in.yaml')
    selected_file = template_file if render_template else direct_file
    selected_file.parent.mkdir()
    selected_file.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n',
        encoding='utf-8',
    )
    ctx = LaunchContext()
    ctx.launch_configurations.update(
        {'config_root': str(tmp_path), 'result_key': 'selected_params', 'robot_prefix': 'fl1_'}
    )
    action = rlh.ResolveParamsFile(
        params_file=PathJoinSubstitution(
            [LaunchConfiguration('config_root'), 'direct', 'settings.yaml']
        ),
        template_params_file=PathJoinSubstitution(
            [LaunchConfiguration('config_root'), 'templates', 'settings.in.yaml']
        ),
        output_context_key=LaunchConfiguration('result_key'),
    )
    children = action.execute(ctx)
    assert len(children) == 1
    assert 'selected_params' not in ctx.launch_configurations
    expected_type = rlh.RenderParamsFile if render_template else SetLaunchConfiguration
    assert isinstance(children[0], expected_type)
    children[0].visit(ctx)
    output_file = Path(ctx.launch_configurations['selected_params'])
    if render_template:
        assert output_file != selected_file
        assert 'fl1_base_link' in output_file.read_text(encoding='utf-8')
        output_file.unlink()
    else:
        assert output_file == selected_file
        assert '$(var robot_prefix)' in output_file.read_text(encoding='utf-8')


@pytest.mark.parametrize('case', ['missing', 'both', 'directory'])
def test_resolve_params_file_rejects_invalid_source_selection(tmp_path, case):
    direct_file = tmp_path.joinpath('direct.yaml')
    template_file = tmp_path.joinpath('template.yaml')
    if case == 'both':
        direct_file.write_text('{}\n', encoding='utf-8')
        template_file.write_text('{}\n', encoding='utf-8')
    elif case == 'directory':
        direct_file.mkdir()
    ctx = LaunchContext()
    ctx.launch_configurations['selected_params'] = 'previous.yaml'
    action = rlh.ResolveParamsFile(
        params_file=str(direct_file), template_params_file=str(template_file),
        output_context_key='selected_params',
    )
    error_type = FileNotFoundError if case == 'missing' else ValueError
    with pytest.raises(error_type):
        action.execute(ctx)
    assert ctx.launch_configurations['selected_params'] == 'previous.yaml'


@pytest.mark.parametrize('argument', ['params_file', 'template_params_file', 'output_context_key'])
def test_resolve_params_file_rejects_empty_inputs(argument):
    kwargs = {
        'params_file': 'direct.yaml', 'template_params_file': 'template.yaml',
        'output_context_key': 'selected_params',
    }
    kwargs[argument] = ''
    with pytest.raises(ValueError, match=argument):
        rlh.ResolveParamsFile(**kwargs).execute(LaunchContext())


def test_resolve_params_file_honors_launch_condition():
    ctx = LaunchContext()
    action = rlh.ResolveParamsFile(
        params_file='missing.yaml', template_params_file='also_missing.yaml',
        output_context_key='selected_params', condition=IfCondition('False'),
    )
    assert action.visit(ctx) is None
    assert 'selected_params' not in ctx.launch_configurations
