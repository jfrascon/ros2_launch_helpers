"""
Test node launch configuration helpers.

These tests verify how ros2_launch_helpers parses node options, logging options, and remapping
configuration maps into the per-node values passed to launch actions.
"""

import json

import pytest

import ros2_launch_helpers as rlh


def _resolve(
    node_names=None,
    node_options=None,
    node_logging_options=None,
    node_remappings=None,
):
    return rlh.resolve_node_launch_configs(
        node_names=node_names or ['front_odometry'],
        node_options=json.dumps(node_options or {}),
        node_logging_options=json.dumps(node_logging_options or {}),
        node_remappings=json.dumps(node_remappings or {}),
    )


def test_default_json_strings_are_empty_global_maps():
    assert rlh.default_node_options_json_str() == '{}'
    assert rlh.default_node_logging_options_json_str() == '{}'
    assert rlh.default_node_remappings_json_str() == '{}'


def test_empty_json_uses_defaults_for_requested_node():
    node_options_by_name, remappings_by_name, ros_arguments_by_name = (
        rlh.resolve_node_launch_configs(
            node_names=['ground_vehicle_twist_odometry'],
            node_options='',
            node_logging_options=None,
            node_remappings='{}',
        )
    )

    assert node_options_by_name['ground_vehicle_twist_odometry'] == rlh.DEFAULT_NODE_OPTIONS
    assert remappings_by_name['ground_vehicle_twist_odometry'] is None
    assert ros_arguments_by_name['ground_vehicle_twist_odometry'] == ['--log-level', 'info']


def test_node_options_partial_entry_merges_with_defaults():
    node_options_by_name, _, _ = _resolve(
        node_options={'front_odometry': {'emulate_tty': False}},
    )

    assert node_options_by_name['front_odometry'] == {
        'output': 'screen',
        'emulate_tty': False,
        'respawn': False,
        'respawn_delay': 0.0,
    }


def test_node_options_complete_entry_overrides_all_default_fields():
    node_options_by_name, _, _ = _resolve(
        node_options={
            'front_odometry': {
                'output': 'both',
                'emulate_tty': False,
                'respawn': True,
                'respawn_delay': 3.5,
            }
        }
    )

    assert node_options_by_name['front_odometry'] == {
        'output': 'both',
        'emulate_tty': False,
        'respawn': True,
        'respawn_delay': 3.5,
    }


def test_node_options_rejects_name():
    with pytest.raises(ValueError, match='unknown key'):
        _resolve(node_options={'front_odometry': {'name': 'other_name'}})


def test_node_logging_options_partial_entry_merges_with_defaults():
    _, _, ros_arguments_by_name = _resolve(
        node_logging_options={'front_odometry': {'disable-stdout-logs': True}},
    )

    assert ros_arguments_by_name['front_odometry'] == [
        '--log-level',
        'info',
        '--disable-stdout-logs',
    ]


def test_node_logging_options_generates_expected_ros_arguments():
    _, _, ros_arguments_by_name = _resolve(
        node_logging_options={
            'front_odometry': {
                'log-level': 'warn',
                'disable-stdout-logs': True,
                'disable-rosout-logs': True,
                'disable-external-lib-logs': True,
            }
        }
    )

    assert ros_arguments_by_name['front_odometry'] == [
        '--log-level',
        'warn',
        '--disable-stdout-logs',
        '--disable-rosout-logs',
        '--disable-external-lib-logs',
    ]


def test_custom_logger_generates_log_level_argument():
    _, _, ros_arguments_by_name = _resolve(
        node_logging_options={'front_odometry': {'some.logger.name': 'debug'}},
    )

    assert ros_arguments_by_name['front_odometry'] == [
        '--log-level',
        'info',
        '--log-level',
        'some.logger.name:=debug',
    ]


def test_node_remappings_converts_json_to_tuples():
    _, remappings_by_name, _ = _resolve(
        node_remappings={
            'front_odometry': [
                '/input_twist:=/cmd_vel',
                'odom:=wheel_odom',
            ]
        }
    )

    assert remappings_by_name['front_odometry'] == [
        ('/input_twist', '/cmd_vel'),
        ('odom', 'wheel_odom'),
    ]


def test_multiple_node_names_return_dictionaries_indexed_by_node_name():
    node_options_by_name, remappings_by_name, ros_arguments_by_name = _resolve(
        node_names=['front_odometry', 'rear_odometry'],
        node_options={
            'front_odometry': {'respawn': True},
            'rear_odometry': {'emulate_tty': False},
        },
        node_logging_options={'rear_odometry': {'log-level': 'debug'}},
        node_remappings={
            'front_odometry': ['odom:=front/odom'],
        },
    )

    assert node_options_by_name['front_odometry']['respawn'] is True
    assert node_options_by_name['rear_odometry']['emulate_tty'] is False
    assert remappings_by_name['front_odometry'] == [('odom', 'front/odom')]
    assert remappings_by_name['rear_odometry'] is None
    assert ros_arguments_by_name['front_odometry'] == ['--log-level', 'info']
    assert ros_arguments_by_name['rear_odometry'] == ['--log-level', 'debug']


def test_unused_invalid_entries_are_ignored():
    node_options_by_name, remappings_by_name, ros_arguments_by_name = _resolve(
        node_names=['used_node'],
        node_options={'unused_node': {'name': 'bad'}},
        node_logging_options={'unused_node': {'disable-stdout-logs': 'true'}},
        node_remappings={'unused_node': 'bad'},
    )

    assert node_options_by_name['used_node'] == rlh.DEFAULT_NODE_OPTIONS
    assert ros_arguments_by_name['used_node'] == ['--log-level', 'info']
    assert remappings_by_name['used_node'] is None


def test_invalid_used_entries_raise_value_error():
    with pytest.raises(ValueError, match='respawn'):
        _resolve(node_options={'front_odometry': {'respawn': 'true'}})

    with pytest.raises(ValueError, match='disable-stdout-logs'):
        _resolve(node_logging_options={'front_odometry': {'disable-stdout-logs': 'true'}})

    with pytest.raises(ValueError, match='from:=to'):
        _resolve(node_remappings={'front_odometry': ['/a']})

    with pytest.raises(ValueError, match="'from'"):
        _resolve(node_remappings={'front_odometry': [':=/b']})

    with pytest.raises(ValueError, match="'to'"):
        _resolve(node_remappings={'front_odometry': ['/a:=']})


@pytest.mark.parametrize('respawn_delay', [-0.1, float('nan'), float('inf')])
def test_node_options_rejects_invalid_respawn_delay(respawn_delay):
    with pytest.raises(ValueError, match='finite number greater than or equal to 0'):
        _resolve(node_options={'front_odometry': {'respawn_delay': respawn_delay}})


def test_invalid_json_raises_value_error():
    with pytest.raises(ValueError, match='valid JSON'):
        rlh.resolve_node_launch_configs(['front_odometry'], '{', '{}', '{}')


def test_non_object_json_root_raises_value_error():
    with pytest.raises(ValueError, match='JSON object'):
        rlh.resolve_node_launch_configs(['front_odometry'], '[]', '{}', '{}')


def test_invalid_node_names_raise_value_error():
    with pytest.raises(ValueError, match='node_names item 0'):
        rlh.resolve_node_launch_configs([''], '{}', '{}', '{}')


def test_old_api_is_not_exported():
    assert not hasattr(rlh, 'default_node_options_str')
    assert not hasattr(rlh, 'default_logging_options_str')
    assert not hasattr(rlh, 'process_node_options')
    assert not hasattr(rlh, 'process_node_logging_options')
    assert not hasattr(rlh, 'process_remappings')
    assert not hasattr(rlh, 'default_node_names_json_str')
    assert not hasattr(rlh, 'NODE_NAMES_DESC')
    assert not hasattr(rlh, 'NodeLaunchConfig')
