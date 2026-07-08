import pytest
import ros2_launch_helpers as rlh


def test_require_list_accepts_empty_list():
    rlh.require_list([])


def test_require_list_raises_type_error_for_non_list():
    with pytest.raises(TypeError, match="Expected a list. Got: 'dict'"):
        rlh.require_list({})


def test_require_mapping_accepts_empty_mapping():
    rlh.require_mapping({})


def test_require_mapping_raises_type_error_for_non_mapping():
    with pytest.raises(TypeError, match="Expected a mapping. Got: 'list'"):
        rlh.require_mapping([])


def test_require_non_empty_list_accepts_non_empty_list():
    rlh.require_non_empty_list(['value'])


def test_require_non_empty_list_raises_value_error_for_empty_list():
    with pytest.raises(ValueError, match='Expected a non-empty list'):
        rlh.require_non_empty_list([])


def test_require_non_empty_mapping_accepts_non_empty_mapping():
    rlh.require_non_empty_mapping({'key': 'value'})


def test_require_non_empty_mapping_raises_value_error_for_empty_mapping():
    with pytest.raises(ValueError, match='Expected a non-empty mapping'):
        rlh.require_non_empty_mapping({})
