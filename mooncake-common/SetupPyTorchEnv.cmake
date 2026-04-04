# SetupPyTorchEnv.cmake
#
# This file provides helper functions for building Mooncake Pytorch extensions
# and is meant to be included by BuildEpExt.cmake and BuildPgExt.cmake.

# Ensure we have the correct Python interpreter (respects active virtualenvs)
find_package(Python3 REQUIRED COMPONENTS Interpreter)

# Install PyTorch for a specific version with proper CUDA compatibility handling.
#
# Usage:
#   install_pytorch_wheel("<VERSION>" <CUDA_MAJOR> <CUDA_MINOR> "<MODULE_PREFIX>")
#
# Example:
#   install_pytorch_wheel("2.11.0" 12 8 "[EP]")
function(install_pytorch_wheel _version _cuda_major _cuda_minor _module_prefix)
  message(STATUS "${_module_prefix} Installing PyTorch ${_version} via pip...")

  set(_cu_tag "")

  # Determine the specific CUDA tag for PyTorch wheels
  if(_cuda_major GREATER_EQUAL 13)
    # TODO: Fix when we need to support more CUDA 13 versions or when the CI env is fixed.
    set(_cu_tag "cu130")

  elseif(_cuda_major EQUAL 12 AND _version VERSION_GREATER_EQUAL "2.11.0")
    # PyTorch 2.11.0+ defaults to CUDA 13.
    # We must explicitly point to CUDA 12 wheels for these newer versions.
    if(_cuda_minor GREATER_EQUAL 8)
      set(_cu_tag "cu128")
    elseif(_cuda_minor GREATER_EQUAL 6)
      set(_cu_tag "cu126")
    else()
      message(FATAL_ERROR
        "${_module_prefix} Can't find a matching PyTorch wheel for version ${_version} "
        "with CUDA ${_cuda_major}.${_cuda_minor}"
      )
    endif()
  endif()

  # Construct pip command using the absolute path to the Python executable
  set(_pip_cmd ${Python3_EXECUTABLE} -m pip install "torch==${_version}")

  if(_cu_tag)
    set(_index_url "https://download.pytorch.org/whl/${_cu_tag}")
    message(STATUS "${_module_prefix} Using specific CUDA wheel: ${_index_url}")
    list(APPEND _pip_cmd --index-url "${_index_url}")
  else()
    message(STATUS "${_module_prefix} Using default PyPI wheels for PyTorch ${_version}")
  endif()

  # Execute pip install
  execute_process(
    COMMAND ${_pip_cmd}
    RESULT_VARIABLE _ret
  )

  if(NOT _ret EQUAL 0)
    message(FATAL_ERROR "${_module_prefix} Failed to install PyTorch ${_version}."
            " Command run: '${_pip_cmd}'")
  endif()

  message(STATUS "${_module_prefix} PyTorch ${_version} is ready.")
endfunction()
