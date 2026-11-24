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
qt_add_library(cluster_client STATIC
    api/masterclient.cpp
    api/poller.cpp
    models/monitorstore.cpp
)
target_include_directories(cluster_client PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(cluster_client PUBLIC Qt6::Network)

qt_add_executable(cluster-monitor WIN32
    src/main.cpp
    src/mainwindow.cpp
    views/settings.cpp
    views/dashboard.cpp
    views/diagnostics.cpp
    views/events.cpp
    views/historybar.cpp
    models/nodetable.cpp
    models/eventtable.cpp
    widgets/scorechart.cpp
)
target_link_libraries(cluster-monitor PRIVATE cluster_client Qt6::Widgets)

include(CTest)
if(BUILD_TESTING)
    find_package(Qt6 REQUIRED COMPONENTS Test)
    qt_add_executable(cluster-client-test tests/client_test.cpp)
    target_link_libraries(cluster-client-test PRIVATE cluster_client Qt6::Test)
    add_test(NAME cluster-client COMMAND cluster-client-test)
endif()
