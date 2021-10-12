cmake_minimum_required(VERSION 3.5)
project(google-cloud-cpp-spanner-benchmark CXX C)

# In this example we only require C++11, you can use newer versions in your own
# project:
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(google_cloud_cpp_spanner REQUIRED)

# MSVC requires some additional code to select the correct runtime library
if (VCPKG_TARGET_TRIPLET MATCHES "-static$")
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
else ()
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
endif ()

# Define your targets.
add_executable(benchmark benchmark.cc)
target_link_libraries(benchmark google-cloud-cpp::spanner)