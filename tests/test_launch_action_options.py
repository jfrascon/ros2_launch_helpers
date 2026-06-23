import json
import math

import pytest

import ros2_launch_helpers as rlh


def _resolve(value):
    return rlh.resolve_launch_action_options(json.dumps(value))


def test_default_launch_action_options_json_str_is_empty_object():
    assert rlh.default_launch_action_options_json_str() == '{}'


def test_empty_launch_action_options_returns_empty_dictionary():
    assert rlh.resolve_launch_action_options(None) == {}
    assert rlh.resolve_launch_action_options('') == {}
    assert rlh.resolve_launch_action_options('{}') == {}


def test_get_launch_action_options_merges_defaults_before_json_object():
    launch_action_options = _resolve({'bridge': {'name': 'custom_bridge', 'respawn': True}})

    assert rlh.get_launch_action_options(
        launch_action_options, 'bridge', defaults={'name': 'bridge', 'output': 'screen', 'respawn': False}
    ) == {'name': 'custom_bridge', 'output': 'screen', 'respawn': True}


def test_get_launch_action_options_returns_defaults_for_missing_object():
    launch_action_options = _resolve({'bridge': {'output': 'screen'}})

    assert rlh.get_launch_action_options(launch_action_options, 'rsp', defaults={'name': 'robot_state_publisher'}) == {
        'name': 'robot_state_publisher'
    }


def test_resolve_launch_action_options_accepts_supported_node_fields():
    launch_action_options = _resolve(
        {
            'bridge': {
                'name': 'bridge',
                'exec_name': 'bridge_process',
                'remappings': [['battery_state', 'state/battery']],
                'ros_arguments': ['--log-level', 'debug'],
                'arguments': ['--foo', 'bar'],
            }
        }
    )

    assert launch_action_options['bridge'] == {
        'name': 'bridge',
        'exec_name': 'bridge_process',
        'remappings': [('battery_state', 'state/battery')],
        'ros_arguments': ['--log-level', 'debug'],
        'arguments': ['--foo', 'bar'],
    }


def test_resolve_launch_action_options_accepts_execute_process_fields():
    launch_action_options = _resolve(
        {
            'gazebo': {
                'name': 'gazebo_process',
                'prefix': 'gdb -ex run --args',
                'cwd': '/tmp',
                'env': {'A': '1'},
                'additional_env': {'RCUTILS_COLORIZED_OUTPUT': '1'},
            }
        }
    )

    assert launch_action_options['gazebo'] == {
        'name': 'gazebo_process',
        'prefix': 'gdb -ex run --args',
        'cwd': '/tmp',
        'env': {'A': '1'},
        'additional_env': {'RCUTILS_COLORIZED_OUTPUT': '1'},
    }


def test_resolve_launch_action_options_accepts_execute_local_fields():
    launch_action_options = _resolve(
        {
            'bridge': {
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
        }
    )

    assert launch_action_options['bridge'] == {
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
    launch_action_options = _resolve(
        {
            'bridge': {
                'name': ['bridge'],
                'exec_name': ['bridge', '_process'],
                'prefix': ['gdb', ' -ex run --args'],
                'cwd': ['/tmp'],
                'sigterm_timeout': ['2', '.', '5'],
                'sigkill_timeout': ['1', '0'],
                'output': ['scr', 'een'],
            }
        }
    )

    assert launch_action_options['bridge']['name'] == ['bridge']
    assert launch_action_options['bridge']['sigterm_timeout'] == ['2', '.', '5']


def test_optional_fields_accept_null():
    launch_action_options = _resolve(
        {
            'bridge': {
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
        }
    )

    assert launch_action_options['bridge'] == {
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


def test_resolve_launch_action_options_rejects_invalid_json():
    with pytest.raises(ValueError, match='valid JSON'):
        rlh.resolve_launch_action_options('{')


def test_resolve_launch_action_options_rejects_non_object_root():
    with pytest.raises(ValueError, match='JSON object'):
        rlh.resolve_launch_action_options('[]')


def test_resolve_launch_action_options_rejects_non_object_value():
    with pytest.raises(ValueError, match="object 'bridge'"):
        _resolve({'bridge': []})


@pytest.mark.parametrize('field_name', ['package', 'executable', 'namespace', 'parameters', 'cmd'])
def test_resolve_launch_action_options_rejects_fields_that_belong_in_launch_file(field_name):
    with pytest.raises(ValueError, match='launch file'):
        _resolve({'bridge': {field_name: 'bad'}})


def test_resolve_launch_action_options_rejects_unknown_fields():
    with pytest.raises(ValueError, match='not supported'):
        _resolve({'bridge': {'unknown_field': True}})


@pytest.mark.parametrize('field_name', ['shell', 'emulate_tty', 'cached_output', 'log_cmd', 'respawn'])
def test_bool_fields_reject_strings(field_name):
    with pytest.raises(ValueError, match='boolean'):
        _resolve({'bridge': {field_name: 'true'}})


@pytest.mark.parametrize('field_name', ['shell', 'emulate_tty', 'cached_output', 'log_cmd', 'respawn'])
def test_bool_fields_reject_null(field_name):
    with pytest.raises(ValueError, match='boolean'):
        _resolve({'bridge': {field_name: None}})


@pytest.mark.parametrize('field_name', ['ros_arguments', 'arguments'])
def test_string_list_fields_reject_non_lists(field_name):
    with pytest.raises(ValueError, match='must be a list'):
        _resolve({'bridge': {field_name: '--bad'}})


@pytest.mark.parametrize('field_name', ['ros_arguments', 'arguments'])
def test_string_list_fields_reject_non_string_items(field_name):
    with pytest.raises(ValueError, match='item 0'):
        _resolve({'bridge': {field_name: [1]}})


@pytest.mark.parametrize('field_name', ['name', 'exec_name', 'prefix', 'cwd', 'sigterm_timeout', 'output'])
def test_some_substitutions_type_fields_reject_non_strings_and_non_lists(field_name):
    with pytest.raises(ValueError, match='string or list of strings'):
        _resolve({'bridge': {field_name: 1}})


@pytest.mark.parametrize('field_name', ['sigterm_timeout', 'sigkill_timeout', 'output'])
def test_required_some_substitutions_type_fields_reject_null(field_name):
    with pytest.raises(ValueError, match='string or list of strings'):
        _resolve({'bridge': {field_name: None}})


@pytest.mark.parametrize('field_name', ['env', 'additional_env'])
def test_environment_fields_reject_non_string_values(field_name):
    with pytest.raises(ValueError, match='must be a string'):
        _resolve({'bridge': {field_name: {'A': 1}}})


def test_remappings_reject_non_list_value():
    with pytest.raises(ValueError, match='must be a JSON list'):
        _resolve({'bridge': {'remappings': 'a:=b'}})


def test_remappings_reject_items_that_are_not_pairs():
    with pytest.raises(ValueError, match='two-item list'):
        _resolve({'bridge': {'remappings': [['a']]}})


def test_remappings_reject_empty_source_or_target():
    with pytest.raises(ValueError, match='source'):
        _resolve({'bridge': {'remappings': [['', 'b']]}})

    with pytest.raises(ValueError, match='target'):
        _resolve({'bridge': {'remappings': [['a', '']]}})


@pytest.mark.parametrize('value', ['2.0', True, math.nan, math.inf])
def test_respawn_delay_rejects_invalid_values(value):
    with pytest.raises(ValueError, match='number|finite'):
        _resolve({'bridge': {'respawn_delay': value}})


def test_respawn_delay_accepts_null():
    assert _resolve({'bridge': {'respawn_delay': None}})['bridge']['respawn_delay'] is None


def test_output_format_rejects_null():
    with pytest.raises(ValueError, match='string'):
        _resolve({'bridge': {'output_format': None}})


@pytest.mark.parametrize('value', [1.5, True, '3'])
def test_respawn_max_retries_rejects_non_integer_values(value):
    with pytest.raises(ValueError, match='integer'):
        _resolve({'bridge': {'respawn_max_retries': value}})


def test_respawn_max_retries_rejects_null():
    with pytest.raises(ValueError, match='integer'):
        _resolve({'bridge': {'respawn_max_retries': None}})


def test_old_node_options_api_is_not_exported():
    assert not hasattr(rlh, 'DEFAULT_LOGGING_OPTIONS')
    assert not hasattr(rlh, 'DEFAULT_NODE_OPTIONS')
    assert not hasattr(rlh, 'LOGGING_OPTIONS_DESC')
    assert not hasattr(rlh, 'NODE_OPTIONS_DESC')
    assert not hasattr(rlh, 'REMAPPINGS_DESC')
    assert not hasattr(rlh, 'default_node_options_json_str')
    assert not hasattr(rlh, 'default_node_logging_options_json_str')
    assert not hasattr(rlh, 'default_node_remappings_json_str')
    assert not hasattr(rlh, 'resolve_node_launch_configs')


def test_old_launch_kwargs_api_is_not_exported():
    assert not hasattr(rlh, 'LAUNCH_KWARGS_DESC')
    assert not hasattr(rlh, 'default_launch_kwargs_json_str')
    assert not hasattr(rlh, 'get_launch_kwargs')
    assert not hasattr(rlh, 'resolve_launch_kwargs_map')
