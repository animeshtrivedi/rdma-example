# Author : Animesh Trivedi

cmake_minimum_required (VERSION 2.6)

project (rdma-example)

set(PROJECT_SOURCE_DIR ${CMAKE_SOURCE_DIR}/src)
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/bin)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_BUILD_TYPE Release) # or debug if you need to use gdb on it.

# Some how g++ has better performance than clang++. Of course I don't know all flags for clang++.
set(CMAKE_CXX_COMPILER g++) # or clang++
#set(CMAKE_CXX_COMPILER clang++) # or clang++
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Ofast -ffast-math -funroll-loops -march=native") # get crazy here
#add_compile_options("-std=c++17")
#set(CMAKE_CXX_FLAGS "-O0  -ggdb")

find_library(IBVERBS_LIBRARY ibverbs HINTS /home/atr/local/lib)
find_library(RDMACM_LIBRARY rdmacm HINTS /home/atr/local/lib)

link_libraries(pthread ${IBVERBS_LIBRARY} ${RDMACM_LIBRARY})

include_directories("${PROJECT_SOURCE_DIR}" "/home/atr/local/include/")

add_executable(rdma_server ${PROJECT_SOURCE_DIR}/rdma_common.c ${PROJECT_SOURCE_DIR}/rdma_server.c)
add_executable(rdma_client ${PROJECT_SOURCE_DIR}/rdma_common.c ${PROJECT_SOURCE_DIR}/rdma_client.c)

