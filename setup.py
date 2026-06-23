from setuptools import find_packages, setup

package_name = 'ros2_launch_helpers'

setup(
    name=package_name,
    version='1.0.0',
    packages=find_packages(exclude=['test', 'tests']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name + '/doc', ['doc/launch_action_arguments_design.md']),
    ],
    install_requires=['setuptools', 'PyYAML'],
    zip_safe=True,
    maintainer='Juan Francisco Rascon Crespo',
    maintainer_email='jfracon@gmail.com',
    description='Helpers for ROS 2 launch files: params overlays, launch action arguments, and CLI overrides',
    license='Apache-2.0',
    extras_require={'test': ['pytest']},
    entry_points={'console_scripts': []},
    python_requires='>=3.10',
)
