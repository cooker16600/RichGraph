cmake_minimum_required(VERSION 3.16)
project(RichGraphPackageConsumer LANGUAGES CXX)

find_package(RichGraph CONFIG REQUIRED)

add_executable(richgraph_package_consumer main.cpp)
target_link_libraries(richgraph_package_consumer PRIVATE RichGraph::RichGraph)
