"""Register ROS 2 linters in the package's pytest test path."""

from pathlib import Path
import subprocess

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PYTHON_PATHS = [
    str(PACKAGE_ROOT / 'ros2_launch_helpers'),
    str(PACKAGE_ROOT / 'tests'),
    str(PACKAGE_ROOT / 'setup.py'),
]


def _run_linter(command: list[str]) -> None:
    """Run one ament linter outside pytest and report its complete output on failure."""
    result = subprocess.run(
        command, cwd=PACKAGE_ROOT, capture_output=True, check=False, text=True, timeout=30
    )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.flake8
@pytest.mark.linter
def test_flake8() -> None:
    """Validate Python code with the same Flake8 configuration used by pre-commit."""
    _run_linter(['ament_flake8', '--config', str(PACKAGE_ROOT / 'ament_flake8.ini'), *PYTHON_PATHS])


@pytest.mark.linter
@pytest.mark.pep257
def test_pep257() -> None:
    """Validate Python docstrings through the ROS 2 PEP 257 wrapper."""
    _run_linter(['ament_pep257', *PYTHON_PATHS])


@pytest.mark.linter
@pytest.mark.xmllint
def test_xmllint() -> None:
    """Validate the ROS package manifest through the ROS 2 XML wrapper."""
    _run_linter(['ament_xmllint', str(PACKAGE_ROOT / 'package.xml')])
