"""
Test package metadata that is visible to ROS and Python callers.
"""

import ast
from pathlib import Path
from xml.etree import ElementTree

import pytest
import ros2_launch_helpers as rlh

PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def test_python_public_version_matches_packaging_version():
    setup_tree = ast.parse(PACKAGE_ROOT.joinpath('setup.py').read_text(encoding='utf-8'))
    setup_call = next(node for node in ast.walk(setup_tree) if isinstance(node, ast.Call))
    setup_version = next(
        keyword.value.value
        for keyword in setup_call.keywords
        if keyword.arg == 'version' and isinstance(keyword.value, ast.Constant)
    )

    package_xml_version = ElementTree.parse(PACKAGE_ROOT.joinpath('package.xml')).getroot().findtext('version')

    assert rlh.__version__ == setup_version == package_xml_version


def test_setup_and_package_xml_descriptions_match():
    setup_tree = ast.parse(PACKAGE_ROOT.joinpath('setup.py').read_text(encoding='utf-8'))
    setup_call = next(node for node in ast.walk(setup_tree) if isinstance(node, ast.Call))
    setup_description = next(
        keyword.value.value
        for keyword in setup_call.keywords
        if keyword.arg == 'description' and isinstance(keyword.value, ast.Constant)
    )

    package_xml_description = ElementTree.parse(PACKAGE_ROOT.joinpath('package.xml')).getroot().findtext('description')

    assert setup_description == ' '.join(package_xml_description.split())


def test_package_xml_uses_ament_python_buildtool():
    package_xml_root = ElementTree.parse(PACKAGE_ROOT.joinpath('package.xml')).getroot()
    buildtool_depends = [element.text for element in package_xml_root.findall('buildtool_depend')]
    build_type = package_xml_root.findtext('./export/build_type')

    assert build_type == 'ament_python'
    assert buildtool_depends == ['ament_python']


def test_package_xml_declares_rclpy_runtime_dependency():
    package_xml_root = ElementTree.parse(PACKAGE_ROOT.joinpath('package.xml')).getroot()
    exec_depends = {element.text for element in package_xml_root.findall('exec_depend')}

    assert 'rclpy' in exec_depends


def test_package_xml_declares_only_registered_test_dependencies():
    package_xml_root = ElementTree.parse(PACKAGE_ROOT.joinpath('package.xml')).getroot()
    test_depends = {element.text for element in package_xml_root.findall('test_depend')}

    assert test_depends == {'python3-pytest'}


def test_current_name_helpers_are_exported():
    assert rlh.make_robot_namespace('/robots', 'robot_1') == '/robots/robot_1'
    assert rlh.make_robot_prefix('robot_1') == 'robot_1_'
    assert rlh.validate_name_segment('robot_1') is True
    assert rlh.validate_namespace('robots/robot_1') is True


@pytest.mark.parametrize(
    'removed_name',
    [
        'compute_global_namespace',
        'compute_robot_namespace',
        'compute_robot_prefix',
        'get_parameters',
        'is_valid_name',
        'is_valid_namespace',
        'is_valid_topic_name',
        'make_fully_qualified_node_name',
        'make_fully_qualified_topic_name',
        'resolve_name',
        'resolve_namespace',
        'resolve_topic_name',
    ],
)
def test_removed_helpers_are_not_exported(removed_name):
    assert not hasattr(rlh, removed_name)
