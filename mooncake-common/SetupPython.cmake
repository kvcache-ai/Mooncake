# SetupPython.cmake — resolve the Python interpreter for execute_process() calls.
#
# Honour -DPython3_EXECUTABLE=... when provided (e.g. Docker builds that
# install a non-system Python via deadsnakes), otherwise fall back to the
# default "python3" on PATH.  Sets PYTHON_EXECUTABLE for legacy callers.

if(NOT Python3_EXECUTABLE)
    set(Python3_EXECUTABLE "python3")
endif()
set(PYTHON_EXECUTABLE "${Python3_EXECUTABLE}")
