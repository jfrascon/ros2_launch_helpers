"""
Test namespace helper functions.
"""

import pytest
from rclpy.exceptions import InvalidNamespaceException

import ros2_launch_helpers as rlh


@pytest.mark.parametrize('namespace', ['', '/', 'robot', '/robot', 'robot/front', '/robot/front'])
def test_validate_namespace_accepts_launch_namespace_forms(namespace):
    assert rlh.validate_namespace(namespace) is True


@pytest.mark.parametrize('namespace', [None, 123, [], {}])
def test_validate_namespace_rejects_non_string_values(namespace):
    with pytest.raises(TypeError, match='namespace must be a string'):
        rlh.validate_namespace(namespace)


@pytest.mark.parametrize('namespace', ['robot/', '/robot/', 'robot//front', '1robot', 'robot-name'])
def test_validate_namespace_delegates_invalid_values_to_rclpy(namespace):
    with pytest.raises(InvalidNamespaceException):
        rlh.validate_namespace(namespace)


@pytest.mark.parametrize(
    ('namespace', 'expected'),
    [
        ('', '/'),
        ('/', '/'),
        ('robot', '/robot'),
        ('/robot', '/robot'),
        ('robot/front', '/robot/front'),
        ('/robot/front', '/robot/front'),
        ('/_hidden', '/_hidden'),
    ],
)
def test_make_namespace_absolute_accepts_launch_namespace_forms(namespace, expected):
    assert rlh.make_namespace_absolute(namespace) == expected


@pytest.mark.parametrize('namespace', [None, 123, [], {}])
def test_make_namespace_absolute_rejects_non_string_values(namespace):
    with pytest.raises(TypeError, match='namespace must be a string'):
        rlh.make_namespace_absolute(namespace)


@pytest.mark.parametrize(
    'namespace',
    [
        '//',
        '///',
        'robot/',
        '/robot/',
        '/robot/front/',
        'robot//front',
        '/robot//front',
        '1robot',
        '/robot/1front',
        'robot-name',
        'robot front',
        'róbot',
        'a' * 1000,
    ],
)
def test_make_namespace_absolute_delegates_invalid_values_to_rclpy(namespace):
    with pytest.raises(InvalidNamespaceException):
        rlh.make_namespace_absolute(namespace)


@pytest.mark.parametrize(
    ('namespace', 'robot_name', 'expected'),
    [
        ('', 'robot', 'robot'),
        ('/', 'robot', '/robot'),
        ('fleet', 'robot', 'fleet/robot'),
        ('/fleet', 'robot', '/fleet/robot'),
    ],
)
def test_make_robot_namespace_preserves_the_parent_namespace_form(namespace, robot_name, expected):
    assert rlh.make_robot_namespace(namespace, robot_name) == expected


def test_make_robot_namespace_validates_the_combined_length():
    with pytest.raises(InvalidNamespaceException):
        rlh.make_robot_namespace('a' * 200, 'b' * 200)


def test_namespace_separator_helpers_transform_valid_namespaces():
    assert rlh.flatten_namespace('/fleet/robot', '_') == 'fleet_robot'
    assert rlh.replace_separator_in_namespace('/fleet/robot', '_') == '_fleet_robot'


@pytest.mark.parametrize('new_sep', [None, '', '//'])
@pytest.mark.parametrize('namespace', ['', '/', '/fleet/robot'])
def test_namespace_separator_helpers_reject_invalid_replacement_for_every_namespace(namespace, new_sep):
    with pytest.raises(ValueError, match='single character'):
        rlh.flatten_namespace(namespace, new_sep)

    with pytest.raises(ValueError, match='single character'):
        rlh.replace_separator_in_namespace(namespace, new_sep)


def test_flatten_namespace_rejects_namespace_separator_as_replacement():
    with pytest.raises(ValueError, match='single character other than'):
        rlh.flatten_namespace('/fleet/robot', '/')


@pytest.mark.parametrize('namespace', ['', '/', 'fleet/robot', '/fleet/robot'])
def test_replace_separator_in_namespace_accepts_namespace_separator_as_identity(namespace):
    assert rlh.replace_separator_in_namespace(namespace, '/') == namespace


@pytest.mark.parametrize('namespace', ['robot/', '/robot/', '/robot/front/'])
def test_namespace_helpers_delegate_invalid_namespaces_to_rclpy(namespace):
    with pytest.raises(InvalidNamespaceException):
        rlh.flatten_namespace(namespace, '_')

    with pytest.raises(InvalidNamespaceException):
        rlh.replace_separator_in_namespace(namespace, '_')

    with pytest.raises(InvalidNamespaceException):
        rlh.make_robot_namespace(namespace, 'robot')
