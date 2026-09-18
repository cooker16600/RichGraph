cmake_minimum_required(VERSION 3.16)

project(RichGraph VERSION 0.1.0 LANGUAGES CXX)

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING
      "Build type: Debug, Release, RelWithDebInfo, or MinSizeRel" FORCE)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(COUNT_COST "Collect internal operation-cost counters" OFF)
option(RICHGRAPH_BUILD_BENCHMARKS
       "Build FinBench, SNB, and performance executables" OFF)
option(RICHGRAPH_BUILD_EXAMPLES "Build public API examples" ON)
option(RICHGRAPH_USE_OPENMP "Enable OpenMP acceleration when available" ON)
option(RICHGRAPH_USE_TCMALLOC "Link tcmalloc when it is available" OFF)
option(RICHGRAPH_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(RICHGRAPH_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(RICHGRAPH_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(RICHGRAPH_ENABLE_COVERAGE "Enable coverage instrumentation" OFF)

if(RICHGRAPH_ENABLE_TSAN AND
   (RICHGRAPH_ENABLE_ASAN OR RICHGRAPH_ENABLE_UBSAN))
  message(FATAL_ERROR
          "ThreadSanitizer cannot be combined with ASan or UBSan")
endif()

find_package(Threads REQUIRED)
find_package(TBB CONFIG QUIET COMPONENTS tbb)
if(NOT TARGET TBB::tbb)
  find_package(TBB REQUIRED COMPONENTS tbb)
endif()
if(NOT TARGET TBB::tbb)
  message(FATAL_ERROR "TBB was found but did not provide the TBB::tbb target")
endif()

# Prefer the package config shipped by gflags. Fall back to the repository's
# legacy FindGFlags module on distributions that do not provide one.
find_package(gflags CONFIG QUIET)
set(RICHGRAPH_GFLAGS_TARGET "")
foreach(candidate
        gflags::gflags
        gflags::gflags_shared
        gflags_shared
        gflags)
  if(TARGET ${candidate})
    set(RICHGRAPH_GFLAGS_TARGET ${candidate})
    break()
  endif()
endforeach()

if(NOT RICHGRAPH_GFLAGS_TARGET)
  find_package(GFlags REQUIRED)
  add_library(RichGraphGFlags UNKNOWN IMPORTED)
  set_target_properties(RichGraphGFlags PROPERTIES
    IMPORTED_LOCATION "${GFLAGS_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${GFLAGS_INCLUDE_DIRS}")
  set(RICHGRAPH_GFLAGS_TARGET RichGraphGFlags)
endif()

if(RICHGRAPH_USE_OPENMP)
  find_package(OpenMP QUIET)
endif()

if(RICHGRAPH_USE_TCMALLOC AND NOT RICHGRAPH_ENABLE_ASAN AND
   NOT RICHGRAPH_ENABLE_UBSAN AND NOT RICHGRAPH_ENABLE_TSAN)
  find_library(RICHGRAPH_TCMALLOC_LIBRARY NAMES tcmalloc)
  if(NOT RICHGRAPH_TCMALLOC_LIBRARY)
    message(WARNING
            "tcmalloc was requested but not found; using the system allocator")
  endif()
else()
  # Alternative allocators interpose the same allocation hooks used by the
  # sanitizers and can hide or misreport ownership errors.
  set(RICHGRAPH_TCMALLOC_LIBRARY "")
endif()

add_library(richgraph_build_options INTERFACE)
target_compile_features(richgraph_build_options INTERFACE cxx_std_17)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(richgraph_build_options INTERFACE
    $<$<CONFIG:Release>:-g;-fno-omit-frame-pointer>
    $<$<CONFIG:RelWithDebInfo>:-fno-omit-frame-pointer>)

  if(RICHGRAPH_ENABLE_ASAN)
    target_compile_options(richgraph_build_options INTERFACE
      -fsanitize=address -fno-omit-frame-pointer)
    target_link_options(richgraph_build_options INTERFACE -fsanitize=address)
  endif()
  if(RICHGRAPH_ENABLE_UBSAN)
    target_compile_options(richgraph_build_options INTERFACE
      -fsanitize=undefined -fno-omit-frame-pointer)
    target_link_options(richgraph_build_options INTERFACE -fsanitize=undefined)
  endif()
  if(RICHGRAPH_ENABLE_TSAN)
    target_compile_options(richgraph_build_options INTERFACE
      -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(richgraph_build_options INTERFACE -fsanitize=thread)
    target_compile_definitions(richgraph_build_options INTERFACE
      RICHGRAPH_THREAD_SANITIZER=1)
  endif()
  if(RICHGRAPH_ENABLE_COVERAGE)
    target_compile_options(richgraph_build_options INTERFACE --coverage)
    target_link_options(richgraph_build_options INTERFACE --coverage)
  endif()
elseif(RICHGRAPH_ENABLE_ASAN OR RICHGRAPH_ENABLE_UBSAN OR
       RICHGRAPH_ENABLE_TSAN OR RICHGRAPH_ENABLE_COVERAGE)
  message(FATAL_ERROR
          "The selected instrumentation requires GCC or Clang")
endif()

add_library(LSMGraph STATIC)
add_library(RichGraph::RichGraph ALIAS LSMGraph)

target_sources(LSMGraph
  PRIVATE
    src/compaction.cpp
    src/lsmstore.cpp
    src/SSTable.cpp
    src/version_set.cpp
    src/storage_internal.cpp
    src/superversion.cpp
    src/MemTable.cpp
    src/MemProperty.cpp
    src/LazyUpdate.cpp
    src/LazyFile.cpp
    src/graph_db.cpp
    src/options.cpp
    src/property_delta.cpp
    src/property_delta_manifest.cpp
    src/property_delta_store.cpp
    src/property_update_manager.cpp
    src/flags.cc
    util/worker_pool.cpp
    util/ThreadLocalRandom.cpp
  PRIVATE
    core/DataStructs.h
    core/lsmstore.h
    core/MemTable.h
    core/MemProperty.h
    core/SSTable.h
    core/types.h
    core/config.h
    core/utils.h
    core/flags.h
    core/compaction.h
    core/version_set.h
    core/storage_internal.h
    core/superversion.h
    core/neighbors.h
    core/edge_iterator_base.h
    core/LazyUpdate.h
    core/LazyFile.h
    core/graph_db.h
    core/cache/BlockManager.h
    core/cache/SSTDataCache.h
    core/graph/edge.h
    core/graph/edge_record.h
    core/graph/vertex.h
    util/arena.h
    util/char_arena.h
    util/lock.h
    util/thread_pool.h
    util/worker_pool.h
    util/ThreadLocalRandom.h)

target_include_directories(LSMGraph
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/core>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/util>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_link_libraries(LSMGraph
  PUBLIC
    Threads::Threads
    TBB::tbb
    ${RICHGRAPH_GFLAGS_TARGET}
    richgraph_build_options)

if(OpenMP_CXX_FOUND)
  target_link_libraries(LSMGraph PUBLIC OpenMP::OpenMP_CXX)
endif()

if(RICHGRAPH_TCMALLOC_LIBRARY)
  target_link_libraries(LSMGraph PUBLIC "${RICHGRAPH_TCMALLOC_LIBRARY}")
endif()

if(COUNT_COST)
  target_compile_definitions(LSMGraph PRIVATE DEBUG_COST)
endif()

function(richgraph_add_executable target source)
  add_executable(${target} ${source})
  target_link_libraries(${target}
    PRIVATE
      RichGraph::RichGraph
      ${RICHGRAPH_GFLAGS_TARGET}
      richgraph_build_options)
endfunction()

function(richgraph_register_assert_test target)
  if(NOT BUILD_TESTING)
    return()
  endif()
  # Legacy tests use assert() as their assertion framework. Keep those checks
  # active in Release until they are migrated to a dedicated test framework.
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(${target} PRIVATE -UNDEBUG)
  endif()
  add_test(NAME ${target} COMMAND ${target})
  set(test_labels "smoke")
  if(ARGC GREATER 1)
    set(test_labels "${ARGV1}")
  endif()
  set_tests_properties(${target} PROPERTIES
    LABELS "${test_labels}"
    TIMEOUT 60)
endfunction()

if(BUILD_TESTING)
  richgraph_add_executable(test_fixed_property_layout
                           test/test_fixed_property_layout.cpp)
  richgraph_add_executable(test_graphdb_e2e test/test_graphdb_e2e.cpp)
  richgraph_add_executable(test_graphdb_concurrency
                           test/test_graphdb_concurrency.cpp)
  richgraph_add_executable(test_options test/test_options.cpp)
  richgraph_add_executable(test_property_delta_file
                           test/test_property_delta_file.cpp)
  richgraph_add_executable(test_property_delta_manifest
                           test/test_property_delta_manifest.cpp)
  richgraph_add_executable(test_property_delta_store
                           test/test_property_delta_store.cpp)
  richgraph_add_executable(test_property_update_manager
                           test/test_property_update_manager.cpp)

  richgraph_register_assert_test(test_fixed_property_layout "smoke;unit")
  richgraph_register_assert_test(test_graphdb_e2e "smoke;integration")
  richgraph_register_assert_test(test_graphdb_concurrency
                                 "integration;concurrency")
  richgraph_register_assert_test(test_options "smoke;unit")
  richgraph_register_assert_test(test_property_delta_file "smoke;unit")
  richgraph_register_assert_test(test_property_delta_manifest "smoke;unit")
  richgraph_register_assert_test(test_property_delta_store "smoke;unit")
  richgraph_register_assert_test(test_property_update_manager "smoke;unit")
endif()

if(RICHGRAPH_BUILD_BENCHMARKS)
  richgraph_add_executable(test_graphdb_finbench
                           test/test_graphdb_finbench.cpp)
  richgraph_add_executable(test_graphdb_snb_v1
                           test/test_graphdb_snb_v1.cpp)
endif()

if(RICHGRAPH_BUILD_EXAMPLES)
  add_executable(richgraph_basic_example examples/basic_usage.cpp)
  target_link_libraries(richgraph_basic_example PRIVATE RichGraph::RichGraph)
endif()

set_target_properties(LSMGraph PROPERTIES
  EXPORT_NAME RichGraph
  OUTPUT_NAME richgraph)

install(
  TARGETS LSMGraph richgraph_build_options
  EXPORT RichGraphTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(
  DIRECTORY include/richgraph
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

set(RICHGRAPH_CONFIG_NEEDS_OPENMP ${OpenMP_CXX_FOUND})
configure_package_config_file(
  cmake/RichGraphConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/RichGraphConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/RichGraph)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/RichGraphConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

install(
  EXPORT RichGraphTargets
  FILE RichGraphTargets.cmake
  NAMESPACE RichGraph::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/RichGraph)
install(
  FILES
    ${CMAKE_CURRENT_BINARY_DIR}/RichGraphConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/RichGraphConfigVersion.cmake
    cmake/FindGFlags.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/RichGraph)

message(STATUS "RichGraph build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "RichGraph C++ compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "RichGraph OpenMP: ${OpenMP_CXX_FOUND}")
message(STATUS "RichGraph tcmalloc: ${RICHGRAPH_TCMALLOC_LIBRARY}")
