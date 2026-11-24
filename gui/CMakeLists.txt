cmake_minimum_required(VERSION 3.21)
project(ClusterMonitor VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
if(MSVC)
    add_compile_options(/utf-8)
endif()

find_package(Qt6 6.4 REQUIRED COMPONENTS Widgets Network)
qt_add_library(cluster_client STATIC
    api/masterclient.cpp
    api/poller.cpp
    models/monitorstore.cpp
)
target_include_directories(cluster_client PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(cluster_client PUBLIC Qt6::Network)

qt_add_library(cluster_ui STATIC
    src/mainwindow.cpp
    src/reviewrun.cpp
    src/style.cpp
    views/settings.cpp
    views/dashboard.cpp
    views/diagnostics.cpp
    views/events.cpp
    views/nodeview.cpp
    views/launchdialog.cpp
    views/historybar.cpp
    views/system.cpp
    models/nodetable.cpp
    models/eventtable.cpp
    widgets/scorechart.cpp
    widgets/utcrange.cpp
    widgets/eventhistogram.cpp
    widgets/v32controls.qrc
)
target_link_libraries(cluster_ui PUBLIC cluster_client Qt6::Widgets)
target_compile_definitions(cluster_ui PRIVATE CAD_GUI_RUNNER="${CMAKE_CURRENT_SOURCE_DIR}/managed-run.ps1")
qt_add_executable(cluster-monitor WIN32 src/main.cpp)
target_link_libraries(cluster-monitor PRIVATE cluster_ui)

include(CTest)
if(BUILD_TESTING)
    find_package(Qt6 REQUIRED COMPONENTS Test)
    qt_add_executable(cluster-client-test tests/client_test.cpp)
    target_link_libraries(cluster-client-test PRIVATE cluster_client Qt6::Test)
    add_test(NAME cluster-client COMMAND cluster-client-test)
    qt_add_executable(cluster-views-test tests/views_test.cpp)
    target_link_libraries(cluster-views-test PRIVATE cluster_ui Qt6::Test)
    add_test(NAME cluster-views COMMAND cluster-views-test)
    set_tests_properties(cluster-views PROPERTIES ENVIRONMENT "QT_QPA_PLATFORM=offscreen")
endif()
