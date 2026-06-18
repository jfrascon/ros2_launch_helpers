"""
Test file path and URI resolution helpers.

These tests verify that resolve_file resolves supported inputs and raises distinct exception types for
each resolution failure that callers may want to handle differently.
"""

from pathlib import Path

import pytest

import ros2_launch_helpers as rlh


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
