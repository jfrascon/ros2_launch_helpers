"""Check explicit parameter-file processing without inferring policy from filenames."""

from pathlib import Path

from launch import LaunchContext
from launch.actions import SetLaunchConfiguration
from launch.conditions import IfCondition
from launch.substitutions import LaunchConfiguration
from launch.substitutions import PathJoinSubstitution
import pytest

import ros2_launch_helpers as rlh


@pytest.mark.parametrize('allow_substs', [True, False, 'True', 'False', 'true', 'false'])
def test_process_params_file_uses_explicit_policy(tmp_path, allow_substs):
    params_file = tmp_path.joinpath('arbitrary.template.yaml')
    params_file.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n',
        encoding='utf-8',
    )
    ctx = LaunchContext()
    ctx.launch_configurations.update(
        {'config_root': str(tmp_path), 'output_key': 'selected_params', 'robot_prefix': 'fl1_'}
    )
    action = rlh.ProcessParamsFile(
        params_file=PathJoinSubstitution(
            [LaunchConfiguration('config_root'), 'arbitrary.template.yaml']
        ),
        allow_substs=allow_substs,
        output_context_key=LaunchConfiguration('output_key'),
    )
    children = action.execute(ctx)
    assert len(children) == 1
    assert 'selected_params' not in ctx.launch_configurations
    renders = allow_substs in (True, 'True', 'true')
    expected_type = rlh.RenderParamsFile if renders else SetLaunchConfiguration
    assert isinstance(children[0], expected_type)
    children[0].visit(ctx)
    selected_file = Path(ctx.launch_configurations['selected_params'])
    if renders:
        assert selected_file != params_file
        assert 'fl1_base_link' in selected_file.read_text(encoding='utf-8')
        selected_file.unlink()
    else:
        assert selected_file == params_file
        assert '$(var robot_prefix)' in selected_file.read_text(encoding='utf-8')


@pytest.mark.parametrize('allow_substs', ['true', 'false'])
def test_process_params_file_resolves_policy_from_context(tmp_path, allow_substs):
    params_file = tmp_path.joinpath('settings.yaml')
    params_file.write_text('{}\n', encoding='utf-8')
    ctx = LaunchContext()
    action = rlh.ProcessParamsFile(
        params_file=str(params_file), allow_substs=LaunchConfiguration('render'),
        output_context_key='selected_params',
    )
    ctx.launch_configurations['render'] = allow_substs
    children = action.execute(ctx)
    expected_type = rlh.RenderParamsFile if allow_substs == 'true' else SetLaunchConfiguration
    assert isinstance(children[0], expected_type)


@pytest.mark.parametrize('allow_substs', [False, True])
@pytest.mark.parametrize('directory', [False, True])
def test_process_params_file_requires_a_file_in_both_modes(tmp_path, allow_substs, directory):
    params_file = tmp_path.joinpath('settings.yaml')
    if directory:
        params_file.mkdir()
    ctx = LaunchContext()
    ctx.launch_configurations['selected_params'] = 'previous.yaml'
    action = rlh.ProcessParamsFile(
        params_file=str(params_file), allow_substs=allow_substs,
        output_context_key='selected_params',
    )
    with pytest.raises(FileNotFoundError):
        action.execute(ctx)
    assert ctx.launch_configurations['selected_params'] == 'previous.yaml'


@pytest.mark.parametrize('invalid_bool', ['maybe', ''])
def test_process_params_file_rejects_invalid_boolean(tmp_path, invalid_bool):
    params_file = tmp_path.joinpath('settings.yaml')
    params_file.write_text('{}\n', encoding='utf-8')
    with pytest.raises(ValueError):
        rlh.ProcessParamsFile(
            params_file=str(params_file), allow_substs=invalid_bool,
            output_context_key='selected_params',
        ).execute(LaunchContext())


@pytest.mark.parametrize('argument', ['params_file', 'output_context_key'])
def test_process_params_file_rejects_empty_paths_and_keys(argument):
    kwargs = {
        'params_file': 'settings.yaml', 'allow_substs': False,
        'output_context_key': 'selected_params',
    }
    kwargs[argument] = ''
    with pytest.raises(ValueError, match=argument):
        rlh.ProcessParamsFile(**kwargs).execute(LaunchContext())


def test_process_params_file_honors_launch_condition():
    ctx = LaunchContext()
    action = rlh.ProcessParamsFile(
        params_file='missing.yaml', allow_substs=True, output_context_key='selected_params',
        condition=IfCondition('False'),
    )
    assert action.visit(ctx) is None
    assert 'selected_params' not in ctx.launch_configurations
