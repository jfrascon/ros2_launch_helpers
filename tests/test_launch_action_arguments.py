import json
import math

import pytest

import ros2_launch_helpers as rlh


def _resolve(value, default_arguments=None):
    return rlh.resolve_launch_action_arguments(json.dumps(value), default_arguments=default_arguments)


def test_default_launch_action_arguments_json_str_is_empty_object():
    assert rlh.default_launch_action_arguments_json_str() == '{}'


def test_current_launch_action_arguments_api_is_exported():
    assert hasattr(rlh, 'LAUNCH_ACTION_ARGUMENTS_DESC')
    assert hasattr(rlh, 'default_launch_action_arguments_json_str')
    assert hasattr(rlh, 'resolve_launch_action_arguments')


def test_empty_launch_action_arguments_returns_empty_dictionary():
    assert rlh.resolve_launch_action_arguments(None) == {}
    assert rlh.resolve_launch_action_arguments('') == {}
    assert rlh.resolve_launch_action_arguments('{}') == {}


def test_empty_launch_action_arguments_returns_default_arguments():
    default_arguments = {'name': 'bridge', 'output': 'screen'}

    assert rlh.resolve_launch_action_arguments(None, default_arguments=default_arguments) == default_arguments
    assert rlh.resolve_launch_action_arguments('', default_arguments=default_arguments) == default_arguments
    assert rlh.resolve_launch_action_arguments('{}', default_arguments=default_arguments) == default_arguments


def test_resolve_launch_action_arguments_merges_default_arguments_before_json_object():
    launch_action_arguments = _resolve(
        {'name': 'custom_bridge', 'respawn': True},
        default_arguments={'name': 'bridge', 'output': 'screen', 'respawn': False},
    )

    assert launch_action_arguments == {'name': 'custom_bridge', 'output': 'screen', 'respawn': True}


def test_json_null_overrides_default_arguments():
    assert _resolve({'name': None}, default_arguments={'name': 'bridge', 'output': 'screen'}) == {
        'name': None,
        'output': 'screen',
    }


def test_resolve_launch_action_arguments_rejects_non_dictionary_default_arguments():
    with pytest.raises(ValueError, match='default_arguments must be a dictionary'):
        rlh.resolve_launch_action_arguments('{}', default_arguments=[])


def test_resolve_launch_action_arguments_accepts_supported_node_fields():
    launch_action_arguments = _resolve(
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


def test_resolve_remappings_rejects_invalid_pairs():
    with pytest.raises(ValueError, match='two-item list'):
        rlh.resolve_remappings('custom_remappings', [['a']])

    with pytest.raises(ValueError, match='source'):
        rlh.resolve_remappings('custom_remappings', [['', 'b']])

    with pytest.raises(ValueError, match='target'):
        rlh.resolve_remappings('custom_remappings', [['a', '']])


def test_resolve_launch_action_arguments_accepts_execute_process_fields():
    launch_action_arguments = _resolve(
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


def test_resolve_launch_action_arguments_accepts_execute_local_fields():
    launch_action_arguments = _resolve(
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
    launch_action_arguments = _resolve(
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
    launch_action_arguments = _resolve(
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


def test_resolve_launch_action_arguments_rejects_invalid_json():
    with pytest.raises(ValueError, match='valid JSON'):
        rlh.resolve_launch_action_arguments('{')


def test_resolve_launch_action_arguments_rejects_non_object_root():
    with pytest.raises(ValueError, match='JSON object'):
        rlh.resolve_launch_action_arguments('[]')


def test_old_keyed_json_shape_is_rejected():
    with pytest.raises(ValueError, match='not supported'):
        _resolve({'bridge': {'output': 'screen'}})


@pytest.mark.parametrize('field_name', ['package', 'executable', 'parameters', 'cmd'])
def test_resolve_launch_action_arguments_rejects_fields_that_belong_in_launch_file(field_name):
    with pytest.raises(ValueError, match='launch file'):
        _resolve({field_name: 'bad'})



def test_resolve_launch_action_arguments_rejects_extra_rejected_arguments():
    with pytest.raises(ValueError, match='respawn'):
        rlh.resolve_launch_action_arguments('{"respawn": true}', extra_rejected_arguments={'respawn'})


def test_resolve_launch_action_arguments_can_reject_namespace_as_extra_argument():
    with pytest.raises(ValueError, match='namespace'):
        rlh.resolve_launch_action_arguments('{"namespace": "robot"}', extra_rejected_arguments={'namespace'})


def test_resolve_launch_action_arguments_rejects_unknown_extra_rejected_arguments():
    with pytest.raises(ValueError, match='not known'):
        rlh.resolve_launch_action_arguments('{}', extra_rejected_arguments={'not_a_launch_action_argument'})


def test_resolve_launch_action_arguments_rejects_non_set_extra_rejected_arguments():
    with pytest.raises(ValueError, match='extra_rejected_arguments must be a set'):
        rlh.resolve_launch_action_arguments('{}', extra_rejected_arguments=['respawn'])

def test_resolve_launch_action_arguments_rejects_unknown_fields():
    with pytest.raises(ValueError, match='not supported'):
        _resolve({'unknown_field': True})


@pytest.mark.parametrize('field_name', ['shell', 'emulate_tty', 'cached_output', 'log_cmd', 'respawn'])
def test_bool_fields_reject_strings(field_name):
    with pytest.raises(ValueError, match='boolean'):
        _resolve({field_name: 'true'})


@pytest.mark.parametrize('field_name', ['shell', 'emulate_tty', 'cached_output', 'log_cmd', 'respawn'])
def test_bool_fields_reject_null(field_name):
    with pytest.raises(ValueError, match='boolean'):
        _resolve({field_name: None})


@pytest.mark.parametrize('field_name', ['ros_arguments', 'arguments'])
def test_string_list_fields_reject_non_lists(field_name):
    with pytest.raises(ValueError, match='must be a list'):
        _resolve({field_name: '--bad'})


@pytest.mark.parametrize('field_name', ['ros_arguments', 'arguments'])
def test_string_list_fields_reject_non_string_items(field_name):
    with pytest.raises(ValueError, match='item 0'):
        _resolve({field_name: [1]})


@pytest.mark.parametrize('field_name', ['name', 'namespace', 'exec_name', 'prefix', 'cwd', 'sigterm_timeout', 'output'])
def test_some_substitutions_type_fields_reject_non_strings_and_non_lists(field_name):
    with pytest.raises(ValueError, match='string or list of strings'):
        _resolve({field_name: 1})


@pytest.mark.parametrize('field_name', ['sigterm_timeout', 'sigkill_timeout', 'output'])
def test_required_some_substitutions_type_fields_reject_null(field_name):
    with pytest.raises(ValueError, match='string or list of strings'):
        _resolve({field_name: None})


@pytest.mark.parametrize('field_name', ['env', 'additional_env'])
def test_environment_fields_reject_non_string_values(field_name):
    with pytest.raises(ValueError, match='must be a string'):
        _resolve({field_name: {'A': 1}})


def test_remappings_reject_non_list_value():
    with pytest.raises(ValueError, match='must be a JSON list'):
        _resolve({'remappings': 'a:=b'})


def test_remappings_reject_items_that_are_not_pairs():
    with pytest.raises(ValueError, match='two-item list'):
        _resolve({'remappings': [['a']]})


def test_remappings_reject_empty_source_or_target():
    with pytest.raises(ValueError, match='source'):
        _resolve({'remappings': [['', 'b']]})

    with pytest.raises(ValueError, match='target'):
        _resolve({'remappings': [['a', '']]})


@pytest.mark.parametrize('value', ['2.0', True, math.nan, math.inf])
def test_respawn_delay_rejects_invalid_values(value):
    with pytest.raises(ValueError, match='number|finite'):
        _resolve({'respawn_delay': value})


def test_respawn_delay_accepts_null():
    assert _resolve({'respawn_delay': None})['respawn_delay'] is None


def test_output_format_rejects_null():
    with pytest.raises(ValueError, match='string'):
        _resolve({'output_format': None})


@pytest.mark.parametrize('value', [1.5, True, '3'])
def test_respawn_max_retries_rejects_non_integer_values(value):
    with pytest.raises(ValueError, match='integer'):
        _resolve({'respawn_max_retries': value})


def test_respawn_max_retries_rejects_null():
    with pytest.raises(ValueError, match='integer'):
        _resolve({'respawn_max_retries': None})


def test_old_node_options_api_is_not_exported():
    assert not hasattr(rlh, 'DEFAULT_LOGGING_OPTIONS')
    assert not hasattr(rlh, 'DEFAULT_NODE_OPTIONS')
    assert not hasattr(rlh, 'LOGGING_OPTIONS_DESC')
    assert not hasattr(rlh, 'NODE_OPTIONS_DESC')
    assert not hasattr(rlh, 'REMAPPINGS_DESC')
    assert not hasattr(rlh, 'default_node_str_json_arguments')
    assert not hasattr(rlh, 'default_node_logging_str_json_arguments')
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
