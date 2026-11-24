cmake_minimum_required(VERSION 3.21)
project(ClusterMonitor VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_AUTOMOC ON)
if(MSVC)
    add_compile_options(/utf-8)
endif()

find_package(Qt6 6.5 REQUIRED COMPONENTS Widgets Network)

qt_add_executable(cluster-monitor WIN32
    src/main.cpp
    src/mainwindow.cpp
    api/masterclient.cpp
)
target_include_directories(cluster-monitor PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(cluster-monitor PRIVATE Qt6::Widgets Qt6::Network)
