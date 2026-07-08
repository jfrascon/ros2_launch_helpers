import inspect
import json
import math

import pytest
import ros2_launch_helpers as rlh
import ros2_launch_helpers.launch_action_arguments as launch_action_arguments
from launch.action import Action
from launch.actions import ExecuteLocal, ExecuteProcess
from launch_ros.actions import Node


def _resolve_node(value, default_arguments=None):
    return rlh.resolve_node_arguments(json.dumps(value), default_arguments=default_arguments)


def _resolve_execute_process(value, default_arguments=None):
    return rlh.resolve_execute_process_arguments(json.dumps(value), default_arguments=default_arguments)


def _resolve_execute_local(value, default_arguments=None):
    return rlh.resolve_execute_local_arguments(json.dumps(value), default_arguments=default_arguments)


def _keyword_only_args(cls: type) -> set[str]:
    signature = inspect.signature(cls.__init__)
    return {
        name
        for name, parameter in signature.parameters.items()
        if name != 'self' and parameter.kind is inspect.Parameter.KEYWORD_ONLY
    }


def test_default_launch_action_arguments_json_str_is_empty_object():
    assert rlh.default_launch_action_arguments_json_str() == '{}'


def test_current_launch_action_arguments_api_is_exported():
    assert hasattr(rlh, 'LAUNCH_ACTION_ARGUMENTS_DESC')
    assert hasattr(rlh, 'default_launch_action_arguments_json_str')
    assert hasattr(rlh, 'resolve_execute_local_arguments')
    assert hasattr(rlh, 'resolve_execute_process_arguments')
    assert hasattr(rlh, 'resolve_node_arguments')


def test_ros2_action_constructor_arguments_are_still_known():
    assert _keyword_only_args(Action) == launch_action_arguments._ACTION_KNOWN_ARGUMENTS
    assert _keyword_only_args(ExecuteLocal) == launch_action_arguments._EXECUTE_LOCAL_DECLARED_ARGUMENTS
    assert _keyword_only_args(ExecuteProcess) == launch_action_arguments._EXECUTE_PROCESS_DECLARED_ARGUMENTS
    assert _keyword_only_args(Node) == launch_action_arguments._NODE_DECLARED_ARGUMENTS


def test_empty_launch_action_arguments_returns_empty_dictionary():
    assert rlh.resolve_node_arguments(None) == {}
    assert rlh.resolve_node_arguments('') == {}
    assert rlh.resolve_node_arguments('{}') == {}


def test_empty_launch_action_arguments_returns_default_arguments():
    default_arguments = {'name': 'bridge', 'output': 'screen'}

    assert rlh.resolve_node_arguments(None, default_arguments=default_arguments) == default_arguments
    assert rlh.resolve_node_arguments('', default_arguments=default_arguments) == default_arguments
    assert rlh.resolve_node_arguments('{}', default_arguments=default_arguments) == default_arguments


def test_resolve_node_arguments_merges_default_arguments_before_json_object():
    launch_action_arguments = _resolve_node(
        {'name': 'custom_bridge', 'respawn': True},
        default_arguments={'name': 'bridge', 'output': 'screen', 'respawn': False},
    )

    assert launch_action_arguments == {'name': 'custom_bridge', 'output': 'screen', 'respawn': True}


def test_resolve_node_arguments_copies_mutable_default_arguments():
    default_arguments = {
        'env': {'ROBOT_NAME': 'front'},
        'prefix': ['xterm', '-e'],
        'ros_arguments': ['--log-level', 'info'],
    }

    launch_action_arguments = rlh.resolve_node_arguments('{}', default_arguments=default_arguments)

    assert launch_action_arguments == default_arguments
    assert launch_action_arguments['env'] is not default_arguments['env']
    assert launch_action_arguments['prefix'] is not default_arguments['prefix']
    assert launch_action_arguments['ros_arguments'] is not default_arguments['ros_arguments']

    launch_action_arguments['env']['ROBOT_NAME'] = 'rear'
    launch_action_arguments['prefix'].append('bash')
    launch_action_arguments['ros_arguments'].append('debug')

    assert default_arguments == {
        'env': {'ROBOT_NAME': 'front'},
        'prefix': ['xterm', '-e'],
        'ros_arguments': ['--log-level', 'info'],
    }


def test_resolve_argument_dict_does_not_copy_mutable_values_when_not_requested():
    argument_value = ['--log-level', 'info']

    launch_action_arguments_result = launch_action_arguments._resolve_argument_dict(
        {'ros_arguments': argument_value},
        known_arguments=launch_action_arguments._NODE_KNOWN_ARGUMENTS,
        rejected_arguments=launch_action_arguments._NODE_REJECTED_ARGUMENTS,
        argument_resolver=launch_action_arguments._resolve_node_argument,
        copy_resolved_mutable_values=False,
    )

    assert launch_action_arguments_result['ros_arguments'] is argument_value


def test_json_null_overrides_default_arguments():
    assert _resolve_node({'name': None}, default_arguments={'name': 'bridge', 'output': 'screen'}) == {
        'name': None,
        'output': 'screen',
    }


def test_resolve_node_arguments_rejects_non_dictionary_default_arguments():
    with pytest.raises(ValueError, match='default_arguments must be a dictionary'):
        rlh.resolve_node_arguments('{}', default_arguments=[])


def test_resolve_node_arguments_resolves_default_argument_values():
    assert rlh.resolve_node_arguments('{}', default_arguments={'respawn_delay': 2}) == {'respawn_delay': 2.0}


@pytest.mark.parametrize('field_name', ['package', 'executable', 'parameters'])
def test_resolve_node_arguments_rejects_default_arguments_that_belong_in_launch_file(field_name):
    with pytest.raises(ValueError, match='launch file'):
        rlh.resolve_node_arguments('{}', default_arguments={field_name: 'bad'})


def test_resolve_node_arguments_rejects_unknown_default_arguments():
    with pytest.raises(ValueError, match='not supported'):
        rlh.resolve_node_arguments('{}', default_arguments={'unknown_field': True})


def test_resolve_execute_process_arguments_rejects_node_default_arguments():
    with pytest.raises(ValueError, match='not supported'):
        rlh.resolve_execute_process_arguments('{}', default_arguments={'ros_arguments': ['--log-level', 'debug']})


def test_resolve_node_arguments_accepts_supported_node_fields():
    launch_action_arguments = _resolve_node(
        {
            'name': 'bridge',
            'namespace': 'robot',
            'exec_name': 'bridge_process',
            'remappings': [['battery_state', 'state/battery']],
            'ros_arguments': ['--log-level', 'debug'],
            'arguments': ['--foo', 'bar'],
        }
    )

    assert launch_action_arguments == {
        'name': 'bridge',
        'namespace': 'robot',
        'exec_name': 'bridge_process',
        'remappings': [('battery_state', 'state/battery')],
        'ros_arguments': ['--log-level', 'debug'],
        'arguments': ['--foo', 'bar'],
    }


def test_resolve_remappings_accepts_none():
    assert rlh.resolve_remappings('remappings', None) is None


def test_resolve_remappings_converts_json_pairs_to_tuples():
    assert rlh.resolve_remappings('remappings', [['battery_state', 'state/battery']]) == [
        ('battery_state', 'state/battery')
    ]


def test_resolve_remappings_accepts_python_tuple_pairs():
    assert rlh.resolve_remappings('remappings', [('battery_state', 'state/battery')]) == [
        ('battery_state', 'state/battery')
    ]


def test_resolve_node_arguments_accepts_python_tuple_remappings_in_default_arguments():
    assert rlh.resolve_node_arguments('{}', default_arguments={'remappings': [('from', 'to')]}) == {
        'remappings': [('from', 'to')]
    }


def test_resolve_remappings_rejects_invalid_pairs():
    with pytest.raises(ValueError, match='two-item list or tuple'):
        rlh.resolve_remappings('custom_remappings', [['a']])

    with pytest.raises(ValueError, match='source'):
        rlh.resolve_remappings('custom_remappings', [['', 'b']])

    with pytest.raises(ValueError, match='target'):
        rlh.resolve_remappings('custom_remappings', [['a', '']])


def test_resolve_execute_process_arguments_accepts_execute_process_fields():
    launch_action_arguments = _resolve_execute_process(
        {
            'name': 'gazebo_process',
            'prefix': 'gdb -ex run --args',
            'cwd': '/tmp',
            'env': {'A': '1'},
            'additional_env': {'RCUTILS_COLORIZED_OUTPUT': '1'},
        }
    )

    assert launch_action_arguments == {
        'name': 'gazebo_process',
        'prefix': 'gdb -ex run --args',
        'cwd': '/tmp',
        'env': {'A': '1'},
        'additional_env': {'RCUTILS_COLORIZED_OUTPUT': '1'},
    }


def test_resolve_execute_local_arguments_accepts_execute_local_fields():
    launch_action_arguments = _resolve_execute_local(
        {
            'shell': False,
            'sigterm_timeout': '2.5',
            'sigkill_timeout': ['1', '0'],
            'emulate_tty': True,
            'output': 'screen',
            'output_format': '[{this.process_description.final_name}] {line}',
            'cached_output': False,
            'log_cmd': True,
            'respawn': True,
            'respawn_delay': 2.0,
            'respawn_max_retries': 3,
        }
    )

    assert launch_action_arguments == {
        'shell': False,
        'sigterm_timeout': '2.5',
        'sigkill_timeout': ['1', '0'],
        'emulate_tty': True,
        'output': 'screen',
        'output_format': '[{this.process_description.final_name}] {line}',
        'cached_output': False,
        'log_cmd': True,
        'respawn': True,
        'respawn_delay': 2.0,
        'respawn_max_retries': 3,
    }


def test_some_substitutions_type_fields_accept_string_or_list_of_strings():
    launch_action_arguments = _resolve_node(
        {
            'name': ['bridge'],
            'exec_name': ['bridge', '_process'],
            'prefix': ['gdb', ' -ex run --args'],
            'cwd': ['/tmp'],
            'sigterm_timeout': ['2', '.', '5'],
            'sigkill_timeout': ['1', '0'],
            'output': ['scr', 'een'],
        }
    )

    assert launch_action_arguments['name'] == ['bridge']
    assert launch_action_arguments['sigterm_timeout'] == ['2', '.', '5']


def test_optional_fields_accept_null():
    launch_action_arguments = _resolve_node(
        {
            'name': None,
            'exec_name': None,
            'remappings': None,
            'ros_arguments': None,
            'arguments': None,
            'prefix': None,
            'cwd': None,
            'env': None,
            'additional_env': None,
            'respawn_delay': None,
        }
    )

    assert launch_action_arguments == {
        'name': None,
        'exec_name': None,
        'remappings': None,
        'ros_arguments': None,
        'arguments': None,
        'prefix': None,
        'cwd': None,
        'env': None,
        'additional_env': None,
        'respawn_delay': None,
    }


def test_resolve_node_arguments_rejects_invalid_json():
    with pytest.raises(ValueError, match='valid JSON'):
        rlh.resolve_node_arguments('{')


def test_resolve_node_arguments_rejects_non_object_root():
    with pytest.raises(ValueError, match='JSON object'):
        rlh.resolve_node_arguments('[]')


def test_old_keyed_json_shape_is_rejected():
    with pytest.raises(ValueError, match='not supported'):
        _resolve_node({'bridge': {'output': 'screen'}})


@pytest.mark.parametrize(
    'field_name', ['condition', 'process_description', 'on_exit', 'cmd', 'package', 'executable', 'parameters']
)
def test_resolve_node_arguments_rejects_fields_that_belong_in_launch_file(field_name):
    with pytest.raises(ValueError, match='launch file'):
        _resolve_node({field_name: 'bad'})


def test_resolve_node_arguments_rejects_extra_rejected_arguments():
    with pytest.raises(ValueError, match='respawn'):
        rlh.resolve_node_arguments('{"respawn": true}', extra_rejected_arguments={'respawn'})


def test_resolve_node_arguments_can_reject_namespace_as_extra_argument():
    with pytest.raises(ValueError, match='namespace'):
        rlh.resolve_node_arguments('{"namespace": "robot"}', extra_rejected_arguments={'namespace'})


def test_resolve_node_arguments_rejects_unknown_extra_rejected_arguments():
    with pytest.raises(ValueError, match='not known'):
        rlh.resolve_node_arguments('{}', extra_rejected_arguments={'not_a_launch_action_argument'})


def test_resolve_node_arguments_rejects_non_set_extra_rejected_arguments():
    with pytest.raises(ValueError, match='extra_rejected_arguments must be a set'):
        rlh.resolve_node_arguments('{}', extra_rejected_arguments=['respawn'])


@pytest.mark.parametrize('extra_rejected_arguments', [{1}, {'not_a_launch_action_argument', 1}, {''}])
def test_resolve_node_arguments_rejects_invalid_extra_rejected_argument_names(extra_rejected_arguments):
    with pytest.raises(ValueError, match='extra_rejected_arguments must contain non-empty string argument names'):
        rlh.resolve_node_arguments('{}', extra_rejected_arguments=extra_rejected_arguments)


def test_resolve_node_arguments_rejects_unknown_fields():
    with pytest.raises(ValueError, match='not supported'):
        _resolve_node({'unknown_field': True})


@pytest.mark.parametrize(
    'field_name',
    ['namespace', 'exec_name', 'remappings', 'ros_arguments', 'arguments', 'package', 'executable', 'parameters'],
)
def test_resolve_execute_process_arguments_rejects_node_fields(field_name):
    with pytest.raises(ValueError, match='not supported'):
        _resolve_execute_process({field_name: 'bad'})


@pytest.mark.parametrize('field_name', ['condition', 'process_description', 'on_exit', 'cmd'])
def test_resolve_execute_process_arguments_rejects_fields_that_belong_in_launch_file(field_name):
    with pytest.raises(ValueError, match='launch file'):
        _resolve_execute_process({field_name: 'bad'})


@pytest.mark.parametrize('field_name', ['name', 'prefix', 'cwd', 'env', 'additional_env', 'cmd'])
def test_resolve_execute_local_arguments_rejects_execute_process_fields(field_name):
    with pytest.raises(ValueError, match='not supported'):
        _resolve_execute_local({field_name: 'bad'})


@pytest.mark.parametrize(
    'field_name',
    ['namespace', 'exec_name', 'remappings', 'ros_arguments', 'arguments', 'package', 'executable', 'parameters'],
)
def test_resolve_execute_local_arguments_rejects_node_fields(field_name):
    with pytest.raises(ValueError, match='not supported'):
        _resolve_execute_local({field_name: 'bad'})


@pytest.mark.parametrize('field_name', ['shell', 'emulate_tty', 'cached_output', 'log_cmd', 'respawn'])
def test_bool_fields_reject_strings(field_name):
    with pytest.raises(ValueError, match='boolean'):
        _resolve_node({field_name: 'true'})


@pytest.mark.parametrize('field_name', ['shell', 'emulate_tty', 'cached_output', 'log_cmd', 'respawn'])
def test_bool_fields_reject_null(field_name):
    with pytest.raises(ValueError, match='boolean'):
        _resolve_node({field_name: None})


@pytest.mark.parametrize('field_name', ['ros_arguments', 'arguments'])
def test_string_list_fields_reject_non_lists(field_name):
    with pytest.raises(ValueError, match='must be a list'):
        _resolve_node({field_name: '--bad'})


@pytest.mark.parametrize('field_name', ['ros_arguments', 'arguments'])
def test_string_list_fields_reject_non_string_items(field_name):
    with pytest.raises(ValueError, match='item 0'):
        _resolve_node({field_name: [1]})


@pytest.mark.parametrize('field_name', ['name', 'namespace', 'exec_name', 'prefix', 'cwd', 'sigterm_timeout', 'output'])
def test_some_substitutions_type_fields_reject_non_strings_and_non_lists(field_name):
    with pytest.raises(ValueError, match='string or list of strings'):
        _resolve_node({field_name: 1})


@pytest.mark.parametrize('field_name', ['sigterm_timeout', 'sigkill_timeout', 'output'])
def test_required_some_substitutions_type_fields_reject_null(field_name):
    with pytest.raises(ValueError, match='string or list of strings'):
        _resolve_node({field_name: None})


@pytest.mark.parametrize('field_name', ['env', 'additional_env'])
def test_environment_fields_reject_non_string_values(field_name):
    with pytest.raises(ValueError, match='must be a string'):
        _resolve_node({field_name: {'A': 1}})


def test_remappings_reject_non_list_value():
    with pytest.raises(ValueError, match='must be a JSON list'):
        _resolve_node({'remappings': 'a:=b'})


def test_remappings_reject_items_that_are_not_pairs():
    with pytest.raises(ValueError, match='two-item list or tuple'):
        _resolve_node({'remappings': [['a']]})


def test_remappings_reject_empty_source_or_target():
    with pytest.raises(ValueError, match='source'):
        _resolve_node({'remappings': [['', 'b']]})

    with pytest.raises(ValueError, match='target'):
        _resolve_node({'remappings': [['a', '']]})


@pytest.mark.parametrize('value', ['2.0', True, math.nan, math.inf])
def test_respawn_delay_rejects_invalid_values(value):
    with pytest.raises(ValueError, match='number|finite'):
        _resolve_node({'respawn_delay': value})


def test_respawn_delay_accepts_null():
    assert _resolve_node({'respawn_delay': None})['respawn_delay'] is None


def test_output_format_rejects_null():
    with pytest.raises(ValueError, match='string'):
        _resolve_node({'output_format': None})


@pytest.mark.parametrize('value', [1.5, True, '3'])
def test_respawn_max_retries_rejects_non_integer_values(value):
    with pytest.raises(ValueError, match='integer'):
        _resolve_node({'respawn_max_retries': value})


def test_respawn_max_retries_rejects_null():
    with pytest.raises(ValueError, match='integer'):
        _resolve_node({'respawn_max_retries': None})


def test_old_node_options_api_is_not_exported():
    assert not hasattr(rlh, 'DEFAULT_LOGGING_OPTIONS')
    assert not hasattr(rlh, 'DEFAULT_NODE_OPTIONS')
    assert not hasattr(rlh, 'LOGGING_OPTIONS_DESC')
    assert not hasattr(rlh, 'NODE_OPTIONS_DESC')
    assert not hasattr(rlh, 'REMAPPINGS_DESC')
    assert not hasattr(rlh, 'default_node_json_str_arguments')
    assert not hasattr(rlh, 'default_node_logging_json_str_arguments')
    assert not hasattr(rlh, 'default_node_remappings_json_str')
    assert not hasattr(rlh, 'resolve_node_launch_configs')


def test_old_launch_kwargs_api_is_not_exported():
    assert not hasattr(rlh, 'LAUNCH_KWARGS_DESC')
    assert not hasattr(rlh, 'default_launch_kwargs_json_str')
    assert not hasattr(rlh, 'get_launch_kwargs')
    assert not hasattr(rlh, 'resolve_launch_kwargs_map')


def test_old_keyed_launch_action_arguments_api_is_not_exported():
    assert not hasattr(rlh, 'default_launch_action_arguments')
    assert not hasattr(rlh, 'get_launch_action_arguments')
    assert not hasattr(rlh, 'resolve_launch_action_arguments')
