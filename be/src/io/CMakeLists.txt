# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/io")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/io")

add_library(IO STATIC
        array_input_stream.cpp
        fd_output_stream.cpp
        fd_input_stream.cpp
        seekable_input_stream.cpp
        readable.cpp
        s3_input_stream.cpp
        s3_output_stream.cpp
        )
