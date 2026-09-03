"""Test resources that callers need from the installed ROS package."""

from pathlib import Path

from ament_index_python.packages import get_package_share_directory


def test_public_documentation_is_installed() -> None:
    """Require the package manifest, user guide, license, and technical design document."""
    package_share = Path(get_package_share_directory('ros2_launch_helpers'))
    expected_resources = (
        'package.xml',
        'README.md',
        'LICENSE',
        'doc/launch_action_arguments_design.md',
    )

    for relative_path in expected_resources:
        assert package_share.joinpath(relative_path).is_file(), relative_path
