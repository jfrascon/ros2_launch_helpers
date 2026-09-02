"""
Test YAML file reading helpers.

These tests verify that read_yaml_file returns parsed YAML data for valid files and raises distinct
exception types for the main failure modes that callers may want to handle differently.
"""

import pytest
import yaml

import ros2_launch_helpers as rlh


def test_read_yaml_file_allows_empty_yaml_data(tmp_path):
    yaml_path = tmp_path.joinpath('empty.yaml')
    yaml_path.write_text('# comments only\n', encoding='utf-8')

    resolved_yaml_file, data = rlh.read_yaml_file(yaml_path)

    assert resolved_yaml_file == str(yaml_path)
    assert data is None


def test_read_yaml_file_raises_file_not_found_for_missing_file(tmp_path):
    yaml_path = tmp_path.joinpath('missing.yaml')

    with pytest.raises(FileNotFoundError, match='does not point to a file'):
        rlh.read_yaml_file(yaml_path)


def test_read_yaml_file_raises_invalid_file_uri_pattern_error_for_invalid_uri():
    with pytest.raises(rlh.InvalidFileUriPatternError, match='Unsupported file URI scheme'):
        rlh.read_yaml_file('http://example.com/config.yaml')


def test_read_yaml_file_raises_null_file_path_error_for_none_path():
    with pytest.raises(rlh.NullFilePathError, match='File path or URI not provided'):
        rlh.read_yaml_file(None)


def test_read_yaml_file_raises_package_not_found_error_for_missing_package():
    with pytest.raises(rlh.PackageNotFoundError):
        rlh.read_yaml_file('package://package_that_should_not_exist/config.yaml')


def test_read_yaml_file_raises_unicode_decode_error_for_non_utf8_file(tmp_path):
    yaml_path = tmp_path.joinpath('invalid-encoding.yaml')
    yaml_path.write_bytes(b'\xff\xfe\xfd')

    with pytest.raises(UnicodeDecodeError):
        rlh.read_yaml_file(yaml_path)


def test_read_yaml_file_raises_null_file_path_error_for_empty_path():
    with pytest.raises(rlh.NullFilePathError, match='File path or URI not provided'):
        rlh.read_yaml_file(' ')


def test_read_yaml_file_raises_yaml_error_for_invalid_syntax(tmp_path):
    yaml_path = tmp_path.joinpath('invalid.yaml')
    yaml_path.write_text('root: [unterminated\n', encoding='utf-8')

    with pytest.raises(yaml.YAMLError):
        rlh.read_yaml_file(yaml_path)


def test_read_yaml_file_returns_resolved_path_and_loaded_data(tmp_path):
    yaml_path = tmp_path.joinpath('config.yaml')
    yaml_path.write_text('enabled: true\nname: battery\n', encoding='utf-8')

    resolved_yaml_file, data = rlh.read_yaml_file(yaml_path)

    assert resolved_yaml_file == str(yaml_path)
    assert data == {'enabled': True, 'name': 'battery'}
