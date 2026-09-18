# - Find ThreadingBuildingBlocks include dirs and libraries
# Use this module by invoking find_package with the form:
#  find_package(TBB
#    [REQUIRED]             # Fail with error if TBB is not found
#    )                      #
# Once done, this will define
#
#  TBB_FOUND - system has TBB
#  TBB_INCLUDE_DIRS - the TBB include directories
#  TBB_LIBRARIES - TBB libraries to be lined, doesn't include malloc or
#                  malloc proxy
#  TBB::tbb - imported target for the TBB library
#
#  TBB_VERSION_MAJOR - Major Product Version Number
#  TBB_VERSION_MINOR - Minor Product Version Number
#  TBB_INTERFACE_VERSION - Engineering Focused Version Number
#  TBB_COMPATIBLE_INTERFACE_VERSION - The oldest major interface version
#                                     still supported. This uses the engineering
#                                     focused interface version numbers.
#
#  TBB_MALLOC_FOUND - system has TBB malloc library
#  TBB_MALLOC_INCLUDE_DIRS - the TBB malloc include directories
#  TBB_MALLOC_LIBRARIES - The TBB malloc libraries to be lined
#  TBB::malloc - imported target for the TBB malloc library
#
#  TBB_MALLOC_PROXY_FOUND - system has TBB malloc proxy library
#  TBB_MALLOC_PROXY_INCLUDE_DIRS = the TBB malloc proxy include directories
#  TBB_MALLOC_PROXY_LIBRARIES - The TBB malloc proxy libraries to be lined
#  TBB::malloc_proxy - imported target for the TBB malloc proxy library
#
#
# This module reads hints about search locations from variables:
#  ENV TBB_ARCH_PLATFORM - for eg. set it to "mic" for Xeon Phi builds
#  ENV TBB_ROOT or just TBB_ROOT - root directory of tbb installation
#  ENV TBB_BUILD_PREFIX - specifies the build prefix for user built tbb
#                         libraries. Should be specified with ENV TBB_ROOT
#                         and optionally...
#  ENV TBB_BUILD_DIR - if build directory is different than ${TBB_ROOT}/build
#
#
# Modified by Robert Maynard from the original OGRE source
#
#-------------------------------------------------------------------
# This file is part of the CMake build system for OGRE
#     (Object-oriented Graphics Rendering Engine)
# For the latest info, see http://www.ogre3d.org/
#
# The contents of this file are placed in the public domain. Feel
# free to make use of it in any way you like.
#-------------------------------------------------------------------
#
#=============================================================================
# Copyright 2010-2012 Kitware, Inc.
# Copyright 2012      Rolf Eike Beer <eike@sf-mail.de>
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file Copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the License for more information.
#=============================================================================
# (To distribute this file outside of CMake, substitute the full
#  License text for the above reference.)


#=============================================================================
#  FindTBB helper functions and macros
#

#====================================================
# Fix the library path in case it is a linker script
#====================================================
function(tbb_extract_real_library library real_library)
  if(NOT UNIX OR NOT EXISTS ${library})
    set(${real_library} "${library}" PARENT_SCOPE)
    return()
  endif()

  #Read in the first 4 bytes and see if they are the ELF magic number
  set(_elf_magic "7f454c46")
  file(READ ${library} _hex_data OFFSET 0 LIMIT 4 HEX)
  if(_hex_data STREQUAL _elf_magic)
    #we have opened a elf binary so this is what
    #we should link to
    set(${real_library} "${library}" PARENT_SCOPE)
    return()
  endif()

  file(READ ${library} _data OFFSET 0 LIMIT 1024)
  if("${_data}" MATCHES "INPUT \\(([^(]+)\\)")
    #extract out the .so name from REGEX MATCH command
    set(_proper_so_name "${CMAKE_MATCH_1}")

    #construct path to the real .so which is presumed to be in the same directory
    #as the input file
    get_filename_component(_so_dir "${library}" DIRECTORY)
    set(${real_library} "${_so_dir}/${_proper_so_name}" PARENT_SCOPE)
  else()
    #unable to determine what this library is so just hope everything works
    #and pass it unmodified.
    set(${real_library} "${library}" PARENT_SCOPE)
  endif()
endfunction()

#===============================================
# Do the final processing for the package find.
#===============================================
macro(findpkg_finish PREFIX TARGET_NAME)
  if (${PREFIX}_INCLUDE_DIR AND ${PREFIX}_LIBRARY)
    set(${PREFIX}_FOUND TRUE)
    set (${PREFIX}_INCLUDE_DIRS ${${PREFIX}_INCLUDE_DIR})
    set (${PREFIX}_LIBRARIES ${${PREFIX}_LIBRARY})
  else ()
    if (${PREFIX}_FIND_REQUIRED AND NOT ${PREFIX}_FIND_QUIETLY)
      message(FATAL_ERROR "Required library ${PREFIX} not found.")
    endif ()
  endif ()

  if (NOT TARGET "TBB::${TARGET_NAME}")
    if (${PREFIX}_LIBRARY_RELEASE)
      tbb_extract_real_library(${${PREFIX}_LIBRARY_RELEASE} real_release)
    endif ()
    if (${PREFIX}_LIBRARY_DEBUG)
      tbb_extract_real_library(${${PREFIX}_LIBRARY_DEBUG} real_debug)
    endif ()
    add_library(TBB::${TARGET_NAME} UNKNOWN IMPORTED)
    set_target_properties(TBB::${TARGET_NAME} PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${${PREFIX}_INCLUDE_DIR}")
    if (${PREFIX}_LIBRARY_DEBUG AND ${PREFIX}_LIBRARY_RELEASE)
      set_target_properties(TBB::${TARGET_NAME} PROPERTIES
        IMPORTED_LOCATION "${real_release}"
        IMPORTED_LOCATION_DEBUG "${real_debug}"
        IMPORTED_LOCATION_RELEASE "${real_release}")
    elseif (${PREFIX}_LIBRARY_RELEASE)
      set_target_properties(TBB::${TARGET_NAME} PROPERTIES
        IMPORTED_LOCATION "${real_release}")
    elseif (${PREFIX}_LIBRARY_DEBUG)
      set_target_properties(TBB::${TARGET_NAME} PROPERTIES
        IMPORTED_LOCATION "${real_debug}")
    endif ()
  endif ()

  #mark the following variables as internal variables
  mark_as_advanced(${PREFIX}_INCLUDE_DIR
                   ${PREFIX}_LIBRARY
                   ${PREFIX}_LIBRARY_DEBUG
                   ${PREFIX}_LIBRARY_RELEASE)
endmacro()

#===============================================
# Generate debug names from given release names
#===============================================
macro(get_debug_names PREFIX)
  foreach(i ${${PREFIX}})
    set(${PREFIX}_DEBUG ${${PREFIX}_DEBUG} ${i}d ${i}D ${i}_d ${i}_D ${i}_debug ${i})
  endforeach()
endmacro()

#===============================================
# See if we have env vars to help us find tbb
#===============================================
macro(getenv_path VAR)
   set(ENV_${VAR} $ENV{${VAR}})
   # replace won't work if var is blank
   if (ENV_${VAR})
     string( REGEX REPLACE "\\\\" "/" ENV_${VAR} ${ENV_${VAR}} )
   endif ()
endmacro()

#===============================================
# Couple a set of release AND debug libraries
#===============================================
macro(make_library_set PREFIX)
  if (${PREFIX}_RELEASE AND ${PREFIX}_DEBUG)
    set(${PREFIX} optimized ${${PREFIX}_RELEASE} debug ${${PREFIX}_DEBUG})
  elseif (${PREFIX}_RELEASE)
    set(${PREFIX} ${${PREFIX}_RELEASE})
  elseif (${PREFIX}_DEBUG)
    set(${PREFIX} ${${PREFIX}_DEBUG})
  endif ()
endmacro()


#=============================================================================
#  Now to actually find TBB
#

# Get path, convert backslashes as ${ENV_${var}}
getenv_path(TBB_ROOT)

# initialize search paths
set(TBB_PREFIX_PATH ${TBB_ROOT} ${ENV_TBB_ROOT})
set(TBB_INC_SEARCH_PATH "")
set(TBB_LIB_SEARCH_PATH "")


# If user built from sources
set(TBB_BUILD_PREFIX $ENV{TBB_BUILD_PREFIX})
if (TBB_BUILD_PREFIX AND ENV_TBB_ROOT)
  getenv_path(TBB_BUILD_DIR)
  if (NOT ENV_TBB_BUILD_DIR)
    set(ENV_TBB_BUILD_DIR ${ENV_TBB_ROOT}/build)
  endif ()

  # include directory under ${ENV_TBB_ROOT}/include
  list(APPEND TBB_LIB_SEARCH_PATH
    ${ENV_TBB_BUILD_DIR}/${TBB_BUILD_PREFIX}_release
    ${ENV_TBB_BUILD_DIR}/${TBB_BUILD_PREFIX}_debug)
endif ()


# For Windows, let's assume that the user might be using the precompiled