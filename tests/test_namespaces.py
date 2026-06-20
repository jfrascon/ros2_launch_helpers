"""
Test namespace validation helpers.
"""

import pytest

import ros2_launch_helpers as rlh
import ros2_launch_helpers.helpers as helpers


@pytest.mark.parametrize('namespace', [None, 123, [], {}])
def test_is_valid_namespace_returns_false_for_non_string_values(namespace):
    assert helpers.is_valid_namespace(namespace) is False


@pytest.mark.parametrize('namespace', ['', '/', 'robot', '/robot', 'robot/front', '/robot/front/'])
def test_is_valid_namespace_accepts_valid_namespaces(namespace):
    assert helpers.is_valid_namespace(namespace) is True


@pytest.mark.parametrize('namespace', ['//', 'robot//front', '1robot', 'robot-name'])
def test_is_valid_namespace_rejects_invalid_namespaces(namespace):
    assert helpers.is_valid_namespace(namespace) is False


def test_resolve_name_reports_invalid_namespace_for_non_string_values():
    with pytest.raises(ValueError, match='Arguments must be strings'):
        rlh.resolve_name(None, 'robot')
