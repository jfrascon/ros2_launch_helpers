"""
Test package metadata that is visible to ROS and Python callers.
"""

import ast
from pathlib import Path
from xml.etree import ElementTree

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


def test_package_xml_uses_ament_python_buildtool():
    package_xml_root = ElementTree.parse(PACKAGE_ROOT.joinpath('package.xml')).getroot()
    buildtool_depends = [element.text for element in package_xml_root.findall('buildtool_depend')]
    build_type = package_xml_root.findtext('./export/build_type')

    assert build_type == 'ament_python'
    assert buildtool_depends == ['ament_python']
