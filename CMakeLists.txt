cmake_minimum_required(VERSION 3.15)
project(EbsBlockCopyToS3Cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(lambda_entry
    lambda_entry.cpp
    block_processor.cpp
    s3_client.cpp
)

# AWS SDK for C++:
# Uncomment and adjust if the SDK is installed on your system.
# find_package(AWSSDK REQUIRED COMPONENTS s3)
# target_link_libraries(lambda_entry PRIVATE ${AWSSDK_LINK_LIBRARIES})

# Link zlib
find_package(ZLIB REQUIRED)
target_link_libraries(lambda_entry PRIVATE ZLIB::ZLIB)
