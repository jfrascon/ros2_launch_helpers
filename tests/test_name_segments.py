"""Test the project name-segment contract."""

import pytest

import ros2_launch_helpers as rlh


@pytest.mark.parametrize('name_segment', ['A', 'vog', 'vog1', 'vog_fl_01', 'robot_', 'robot__1'])
def test_validate_name_segment_accepts_project_entity_names(name_segment):
    assert rlh.validate_name_segment(name_segment) is True


@pytest.mark.parametrize('name_segment', [None, 123, [], {}])
def test_validate_name_segment_rejects_non_string_values(name_segment):
    with pytest.raises(TypeError, match='name_segment must be a string'):
        rlh.validate_name_segment(name_segment)


def test_validate_name_segment_rejects_empty_string():
    with pytest.raises(ValueError, match='name_segment must not be empty'):
        rlh.validate_name_segment('')


@pytest.mark.parametrize('name_segment', ['_vog', '1vog', '~', '{robot}', 'árobot'])
def test_validate_name_segment_requires_an_initial_ascii_letter(name_segment):
    with pytest.raises(ValueError, match='name_segment must start with an ASCII letter'):
        rlh.validate_name_segment(name_segment)


@pytest.mark.parametrize('name_segment', ['vog/front', 'vog#1', 'vog-1', 'vog 1', 'vog\x00other'])
def test_validate_name_segment_rejects_characters_outside_the_project_contract(name_segment):
    with pytest.raises(ValueError, match='contains invalid character'):
        rlh.validate_name_segment(name_segment)


def test_validate_name_segment_does_not_impose_a_length_limit():
    assert rlh.validate_name_segment('a' * 1000) is True


def test_to_prefix_appends_one_separator():
    assert rlh.to_prefix('vog') == 'vog_'
    assert rlh.to_prefix('vog_') == 'vog_'


def test_to_prefix_validates_its_name_segment():
    with pytest.raises(ValueError, match='must start with an ASCII letter'):
        rlh.to_prefix('_vog')


def test_robot_helpers_validate_the_robot_name_as_a_name_segment():
    with pytest.raises(ValueError, match='must start with an ASCII letter'):
        rlh.make_robot_namespace('/robots', '_vog')

    with pytest.raises(ValueError, match='must start with an ASCII letter'):
        rlh.make_robot_prefix('_vog')
