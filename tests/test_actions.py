"""
Test launch actions exposed by ros2_launch_helpers.
"""

from pathlib import Path

import pytest
import ros2_launch_helpers as rlh
from launch import LaunchContext
from launch.conditions import IfCondition
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution, TextSubstitution


def test_set_global_namespace_action_updates_launch_context():
    ctx = LaunchContext()
    ctx.launch_configurations['namespace'] = 'robots/front'

    result = rlh.SetGlobalNamespace(namespace=LaunchConfiguration('namespace'), output_context_key='namespace').execute(
        ctx
    )

    assert result is None
    assert ctx.launch_configurations['namespace'] == '/robots/front'


def test_set_global_namespace_action_accepts_launch_configuration_input():
    ctx = LaunchContext()
    ctx.launch_configurations['namespace'] = 'robots/front'

    result = rlh.SetGlobalNamespace(
        namespace=LaunchConfiguration('namespace'), output_context_key='global_namespace'
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['global_namespace'] == '/robots/front'


def test_set_global_namespace_action_accepts_literal_input():
    ctx = LaunchContext()

    result = rlh.SetGlobalNamespace(namespace='robots/front', output_context_key='global_namespace').execute(ctx)

    assert result is None
    assert ctx.launch_configurations['global_namespace'] == '/robots/front'


def test_set_global_namespace_action_accepts_substitution_output_context_key():
    ctx = LaunchContext()
    ctx.launch_configurations['namespace_key'] = 'global_namespace'

    result = rlh.SetGlobalNamespace(
        namespace='robots/front', output_context_key=LaunchConfiguration('namespace_key')
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['global_namespace'] == '/robots/front'


@pytest.mark.parametrize(
    ('action_type', 'kwargs'),
    [
        (rlh.SetGlobalNamespace, {'namespace': 'robots/front', 'output_context_key': ''}),
        (rlh.SetRobotNamespace, {'namespace': '/robots', 'robot_name': 'front', 'output_context_key': ''}),
        (rlh.SetRobotPrefix, {'robot_name': 'front', 'output_context_key': ''}),
    ],
)
def test_namespace_actions_reject_empty_output_context_key(action_type, kwargs):
    ctx = LaunchContext()

    with pytest.raises(ValueError, match='must resolve to a non-empty launch configuration key'):
        action_type(**kwargs).execute(ctx)


def test_set_robot_namespace_action_updates_launch_context():
    ctx = LaunchContext()
    ctx.launch_configurations['namespace'] = '/robots'
    ctx.launch_configurations['robot_name'] = 'front'

    result = rlh.SetRobotNamespace(
        namespace=LaunchConfiguration('namespace'),
        robot_name=LaunchConfiguration('robot_name'),
        output_context_key='robot_namespace',
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['robot_namespace'] == '/robots/front'


def test_set_robot_namespace_action_accepts_launch_configuration_inputs():
    ctx = LaunchContext()
    ctx.launch_configurations['namespace'] = '/robots'
    ctx.launch_configurations['robot_name'] = 'front'

    result = rlh.SetRobotNamespace(
        namespace=LaunchConfiguration('namespace'),
        robot_name=LaunchConfiguration('robot_name'),
        output_context_key='target_namespace',
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['target_namespace'] == '/robots/front'


def test_set_robot_namespace_action_accepts_literal_inputs():
    ctx = LaunchContext()

    result = rlh.SetRobotNamespace(
        namespace='/robots', robot_name='front', output_context_key='target_namespace'
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['target_namespace'] == '/robots/front'


def test_set_robot_namespace_action_accepts_substitution_output_context_key():
    ctx = LaunchContext()
    ctx.launch_configurations['output_context_key'] = 'target_namespace'

    result = rlh.SetRobotNamespace(
        namespace='/robots', robot_name='front', output_context_key=LaunchConfiguration('output_context_key')
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['target_namespace'] == '/robots/front'


def test_set_robot_prefix_action_updates_launch_context():
    ctx = LaunchContext()
    ctx.launch_configurations['robot_name'] = 'front'

    result = rlh.SetRobotPrefix(
        robot_name=LaunchConfiguration('robot_name'), output_context_key='robot_prefix'
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['robot_prefix'] == 'front_'


def test_set_robot_prefix_action_accepts_launch_configuration_input():
    ctx = LaunchContext()
    ctx.launch_configurations['robot_name'] = 'front'

    result = rlh.SetRobotPrefix(
        robot_name=LaunchConfiguration('robot_name'), output_context_key='target_prefix'
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['target_prefix'] == 'front_'


def test_set_robot_prefix_action_accepts_literal_input():
    ctx = LaunchContext()

    result = rlh.SetRobotPrefix(robot_name='front', output_context_key='target_prefix').execute(ctx)

    assert result is None
    assert ctx.launch_configurations['target_prefix'] == 'front_'


def test_set_robot_prefix_action_accepts_substitution_output_context_key():
    ctx = LaunchContext()
    ctx.launch_configurations['output_context_key'] = 'target_prefix'

    result = rlh.SetRobotPrefix(
        robot_name='front', output_context_key=LaunchConfiguration('output_context_key')
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['target_prefix'] == 'front_'


def test_require_directory_accepts_existing_directory_path_join_substitution(tmp_path):
    directory_path = tmp_path.joinpath('robot')
    directory_path.mkdir()

    ctx = LaunchContext()

    result = rlh.RequireDirectory(path=PathJoinSubstitution([str(tmp_path), 'robot'])).execute(ctx)

    assert result is None


def test_require_directory_rejects_empty_path():
    ctx = LaunchContext()

    with pytest.raises(ValueError, match='must resolve to a non-empty filesystem path'):
        rlh.RequireDirectory(path='').execute(ctx)


def test_require_directory_rejects_file_path(tmp_path):
    file_path = tmp_path.joinpath('params.yaml')
    file_path.write_text('/**/node:\n  ros__parameters:\n    enabled: true\n', encoding='utf-8')

    ctx = LaunchContext()

    with pytest.raises(FileNotFoundError, match='does not exist or is not a directory'):
        rlh.RequireDirectory(path=str(file_path)).execute(ctx)


def test_require_directory_rejects_missing_path(tmp_path):
    missing_path = tmp_path.joinpath('missing')

    ctx = LaunchContext()

    with pytest.raises(FileNotFoundError, match='does not exist or is not a directory'):
        rlh.RequireDirectory(path=str(missing_path)).execute(ctx)


def test_require_file_accepts_existing_file_path_join_substitution(tmp_path):
    file_path = tmp_path.joinpath('params.yaml')
    file_path.write_text('/**/node:\n  ros__parameters:\n    enabled: true\n', encoding='utf-8')

    ctx = LaunchContext()

    result = rlh.RequireFile(path=PathJoinSubstitution([str(tmp_path), 'params.yaml'])).execute(ctx)

    assert result is None


def test_require_file_rejects_directory_path(tmp_path):
    ctx = LaunchContext()

    with pytest.raises(FileNotFoundError, match='does not exist or is not a file'):
        rlh.RequireFile(path=str(tmp_path)).execute(ctx)


def test_require_file_rejects_empty_path():
    ctx = LaunchContext()

    with pytest.raises(ValueError, match='must resolve to a non-empty filesystem path'):
        rlh.RequireFile(path='').execute(ctx)


def test_require_file_rejects_missing_path(tmp_path):
    missing_path = tmp_path.joinpath('missing.yaml')

    ctx = LaunchContext()

    with pytest.raises(FileNotFoundError, match='does not exist or is not a file'):
        rlh.RequireFile(path=str(missing_path)).execute(ctx)


def test_render_params_file_action_renders_substitutions_and_updates_launch_context(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    source_path.write_text(
        '/**/node:\n  ros__parameters:\n    frame_id: $(var robot_prefix)base_link\n', encoding='utf-8'
    )

    ctx = LaunchContext()
    ctx.launch_configurations['params_file'] = str(source_path)
    ctx.launch_configurations['robot_prefix'] = 'robot_1_'

    result = rlh.RenderParamsFile(
        params_file=LaunchConfiguration('params_file'), output_context_key='params_file'
    ).execute(ctx)

    assert result is None
    rendered_path = Path(ctx.launch_configurations['params_file'])

    assert rendered_path != source_path
    assert rendered_path.read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    frame_id: robot_1_base_link\n'
    )


def test_render_params_file_action_can_write_to_separate_output_context_key(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    source_path.write_text('/**/node:\n  ros__parameters:\n    enabled: true\n', encoding='utf-8')

    ctx = LaunchContext()
    ctx.launch_configurations['params_file'] = str(source_path)

    result = rlh.RenderParamsFile(
        params_file=LaunchConfiguration('params_file'), output_context_key='resolved_params_file'
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['params_file'] == str(source_path)
    assert ctx.launch_configurations['resolved_params_file'] != str(source_path)
    assert Path(ctx.launch_configurations['resolved_params_file']).read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    enabled: true\n'
    )


def test_render_params_file_action_accepts_path_join_substitution(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    source_path.write_text('/**/node:\n  ros__parameters:\n    enabled: true\n', encoding='utf-8')

    ctx = LaunchContext()

    result = rlh.RenderParamsFile(
        params_file=PathJoinSubstitution([str(tmp_path), 'source.yaml']), output_context_key='resolved_params_file'
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['resolved_params_file'] != str(source_path)
    assert Path(ctx.launch_configurations['resolved_params_file']).read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    enabled: true\n'
    )


def test_render_params_file_action_accepts_substitution_output_context_key(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    source_path.write_text('/**/node:\n  ros__parameters:\n    enabled: true\n', encoding='utf-8')

    ctx = LaunchContext()
    ctx.launch_configurations['params_file_key'] = 'resolved_params_file'

    result = rlh.RenderParamsFile(
        params_file=PathJoinSubstitution([str(tmp_path), 'source.yaml']),
        output_context_key=LaunchConfiguration('params_file_key'),
    ).execute(ctx)

    assert result is None
    assert ctx.launch_configurations['resolved_params_file'] != str(source_path)
    assert Path(ctx.launch_configurations['resolved_params_file']).read_text(encoding='utf-8') == (
        '/**/node:\n  ros__parameters:\n    enabled: true\n'
    )


def test_render_params_file_action_rejects_empty_output_context_key(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    source_path.write_text('/**/node:\n  ros__parameters:\n    enabled: true\n', encoding='utf-8')

    ctx = LaunchContext()

    with pytest.raises(ValueError, match='must resolve to a non-empty launch configuration key'):
        rlh.RenderParamsFile(
            params_file=PathJoinSubstitution([str(tmp_path), 'source.yaml']), output_context_key=''
        ).execute(ctx)


def test_render_params_file_action_requires_resolved_filesystem_path(tmp_path):
    source_path = tmp_path.joinpath('source.yaml')
    source_path.write_text('/**/node:\n  ros__parameters:\n    enabled: true\n', encoding='utf-8')

    ctx = LaunchContext()
    ctx.launch_configurations['params_file'] = f'file://{source_path}'

    with pytest.raises(FileNotFoundError, match='Params file'):
        rlh.RenderParamsFile(params_file=LaunchConfiguration('params_file'), output_context_key='params_file').execute(
            ctx
        )


def test_actions_accept_condition():
    condition = IfCondition(LaunchConfiguration('enabled'))

    action = rlh.SetRobotPrefix(
        robot_name=LaunchConfiguration('robot_name'), output_context_key='robot_prefix', condition=condition
    )

    assert action.condition is condition


@pytest.mark.parametrize(
    ('action_type', 'kwargs', 'output_context_key', 'expected_value'),
    [
        (
            rlh.SetGlobalNamespace,
            {'namespace': TextSubstitution(text='robots/front'), 'output_context_key': 'namespace'},
            'namespace',
            '/robots/front',
        ),
        (
            rlh.SetRobotNamespace,
            {
                'namespace': TextSubstitution(text='/robots'),
                'robot_name': TextSubstitution(text='front'),
                'output_context_key': 'robot_namespace',
            },
            'robot_namespace',
            '/robots/front',
        ),
        (
            rlh.SetRobotPrefix,
            {'robot_name': TextSubstitution(text='front'), 'output_context_key': 'robot_prefix'},
            'robot_prefix',
            'front_',
        ),
    ],
)
def test_actions_accept_generic_substitutions(action_type, kwargs, output_context_key, expected_value):
    ctx = LaunchContext()

    result = action_type(**kwargs).execute(ctx)

    assert result is None
    assert ctx.launch_configurations[output_context_key] == expected_value


def test_actions_reject_unknown_constructor_keyword():
    with pytest.raises(TypeError, match='unexpected keyword argument'):
        rlh.SetRobotPrefix(robot_name='front', output_context_key='robot_prefix', unknown='value')
