"""
Test file path and URI resolution helpers.

These tests verify that resolve_file resolves supported inputs and raises distinct exception types for
each resolution failure that callers may want to handle differently.
"""

from pathlib import Path

import pytest

import ros2_launch_helpers as rlh
import ros2_launch_helpers.helpers as helpers


def test_resolve_file_expands_regular_path():
    resolved_file = rlh.resolve_file('~/robot.yaml')

    assert resolved_file == str(Path('~/robot.yaml').expanduser())


def test_resolve_file_resolves_absolute_file_uri(tmp_path):
    target_file = tmp_path.joinpath('robot.yaml')

    resolved_file = rlh.resolve_file(f'file://{target_file}')

    assert resolved_file == str(target_file)


def test_resolve_file_raises_null_file_path_error_for_empty_path():
    with pytest.raises(rlh.NullFilePathError, match='File path or URI not provided'):
        rlh.resolve_file(' ')


def test_resolve_file_raises_null_file_path_error_for_none_path():
    with pytest.raises(rlh.NullFilePathError, match='File path or URI not provided'):
        rlh.resolve_file(None)


def test_resolve_file_raises_invalid_file_uri_pattern_error_for_relative_file_uri():
    with pytest.raises(rlh.InvalidFileUriPatternError, match='absolute path'):
        rlh.resolve_file('file://relative/path.yaml')


def test_resolve_file_raises_invalid_file_uri_pattern_error_for_missing_package_path():
    with pytest.raises(rlh.InvalidFileUriPatternError, match='package://<package>/<path>'):
        rlh.resolve_file('package://robot_mima_mkv30')


def test_resolve_file_raises_invalid_file_uri_pattern_error_for_empty_package_name():
    with pytest.raises(rlh.InvalidFileUriPatternError, match='package://<package>/<path>'):
        rlh.resolve_file('package:///config.yaml')


def test_resolve_file_raises_package_not_found_error_for_missing_package():
    with pytest.raises(rlh.PackageNotFoundError):
        rlh.resolve_file('package://package_that_should_not_exist/config.yaml')


def test_resolve_file_raises_invalid_file_uri_pattern_error_for_unsupported_uri_scheme():
    with pytest.raises(rlh.InvalidFileUriPatternError, match='Unsupported file URI scheme'):
        rlh.resolve_file('http://example.com/config.yaml')


def test_resolve_file_resolves_package_uri_inside_package_share(monkeypatch, tmp_path):
    share_path = tmp_path.joinpath('share', 'robot_pkg')
    config_path = share_path.joinpath('config', 'robot.yaml')
    config_path.parent.mkdir(parents=True)
    config_path.write_text('name: robot\n', encoding='utf-8')

    monkeypatch.setattr(helpers, 'get_package_share_directory', lambda package_name: str(share_path))

    resolved_file = rlh.resolve_file('package://robot_pkg/config/robot.yaml')

    assert resolved_file == str(config_path)


def test_resolve_file_rejects_package_uri_that_escapes_package_share(monkeypatch, tmp_path):
    share_path = tmp_path.joinpath('share', 'robot_pkg')
    share_path.mkdir(parents=True)

    monkeypatch.setattr(helpers, 'get_package_share_directory', lambda package_name: str(share_path))

    with pytest.raises(rlh.InvalidFileUriPatternError, match='inside package share directory'):
        rlh.resolve_file('package://robot_pkg/../outside.yaml')
