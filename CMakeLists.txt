cmake_minimum_required(VERSION 3.8)
project(ros2_launch_helpers)

find_package(ament_cmake REQUIRED)
find_package(ament_cmake_python REQUIRED)

ament_python_install_package(${PROJECT_NAME})

install(DIRECTORY doc DESTINATION share/${PROJECT_NAME})

install(FILES README.md LICENSE DESTINATION share/${PROJECT_NAME})

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)

  # Exclude linters that require package-specific configuration.
  list(APPEND AMENT_LINT_AUTO_EXCLUDE ament_cmake_flake8)

  # Register the remaining linters declared as test dependencies in package.xml.
  ament_lint_auto_find_test_dependencies()

  find_package(ament_cmake_flake8 REQUIRED)

  ament_flake8(CONFIG_FILE "${CMAKE_CURRENT_SOURCE_DIR}/ament_flake8.ini")

  ament_add_pytest_test(test_actions tests/test_actions.py TIMEOUT 120)
  ament_add_pytest_test(test_installed_resources tests/test_installed_resources.py TIMEOUT 120)
  ament_add_pytest_test(test_launch_action_arguments tests/test_launch_action_arguments.py TIMEOUT 120)
  ament_add_pytest_test(test_name_segments tests/test_name_segments.py TIMEOUT 120)
  ament_add_pytest_test(test_namespaces tests/test_namespaces.py TIMEOUT 120)
  ament_add_pytest_test(test_package_metadata tests/test_package_metadata.py TIMEOUT 120)
  ament_add_pytest_test(test_read_yaml_file tests/test_read_yaml_file.py TIMEOUT 120)
  ament_add_pytest_test(test_render_params_file tests/test_render_params_file.py TIMEOUT 120)
  ament_add_pytest_test(test_require_data_shapes tests/test_require_data_shapes.py TIMEOUT 120)
  ament_add_pytest_test(test_resolve_file tests/test_resolve_file.py TIMEOUT 120)
endif()

ament_package()
