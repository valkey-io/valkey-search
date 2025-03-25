cmake_minimum_required(VERSION 3.12)
project(MyPythonTests)

# Find Python interpreter
find_package(Python REQUIRED COMPONENTS Interpreter)

# Check if pip is installed
execute_process(
    COMMAND ${Python_EXECUTABLE} -m pip --version
    RESULT_VARIABLE PIP_CHECK
    OUTPUT_QUIET ERROR_QUIET
)

# Install pip if missing
if(NOT PIP_CHECK EQUAL 0)
    message(FATAL_ERROR "pip not found.")
endif()

# Define virtual environment path
set(VENV_DIR "${CMAKE_BINARY_DIR}/venv")

# Step 1: Create virtual environment if it doesn't exist
if(NOT EXISTS "${VENV_DIR}")
    message(STATUS "Creating virtual environment at ${VENV_DIR}")
    execute_process(
        COMMAND ${Python_EXECUTABLE} -m venv ${VENV_DIR}
        RESULT_VARIABLE VENV_RESULT
        OUTPUT_QUIET ERROR_QUIET
    )
    if(NOT VENV_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to create virtual environment.")
    endif()
endif()

# Set paths to virtual environment executables
set(PYTHON_VENV_EXECUTABLE "${VENV_DIR}/bin/python")
set(PIP_VENV_EXECUTABLE "${VENV_DIR}/bin/pip")

# Step 2: Install dependencies from requirements.txt
if(NOT EXISTS "${CMAKE_SOURCE_DIR}/requirements.txt")
    message(FATAL_ERROR "requirements.txt not found! Python dependencies may be missing.")
endif()

message(STATUS "Installing dependencies in virtual environment...")
execute_process(
    COMMAND ${PYTHON_VENV_EXECUTABLE} -m pip install -r ${CMAKE_SOURCE_DIR}/requirements.txt
    RESULT_VARIABLE PIP_RESULT
    OUTPUT_QUIET ERROR_QUIET
)
if(NOT PIP_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to install dependencies from requirements.txt")
endif()

include(CTest)  # Enables test support

# Function to register Python tests
function(add_pytest test_name test_script)
    add_test(
        NAME ${test_name}
        COMMAND ${PYTHON_VENV_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/${test_script}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endfunction()

# Add tests
add_pytest(stability_test stability_test.py)
add_pytest(vector_search_integration_test vector_search_integration_test.py)

# Set timeout for long-running tests
set_tests_properties(stability_test PROPERTIES TIMEOUT 3600)