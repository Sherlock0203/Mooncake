include_guard(GLOBAL)

include(FetchContent)

# URMA/UMDK resolution.
#
# Prefer a system UMDK installation so configure works without downloading a
# second copy and the headers match the liburma ABI used at runtime. A local
# SDK is looked up first (URMA_ROOT / UMDK_ROOT select an explicit one, in
# either the "<root>" or "<root>/include" form); only when the local headers
# are incomplete do we fall back to FetchContent.
#
# The fallback keeps tent_merge's tag (UMDK master): the existing build tree
# is already populated from master, and production hosts are expected to ship
# the latest UMDK installed locally, so an install is never mandatory.
# yuanhao-1113 pinned GIT_TAG to v25.12.0.B081 here; that pin is deliberately
# NOT adopted so that an already configured master-based build keeps
# resolving identically.
set(_URMA_LOCAL_INCLUDE_DIRS)
foreach(_urma_root ${URMA_ROOT} $ENV{URMA_ROOT} ${UMDK_ROOT} $ENV{UMDK_ROOT})
  if(_urma_root)
    list(APPEND _URMA_LOCAL_INCLUDE_DIRS "${_urma_root}"
         "${_urma_root}/include")
  endif()
endforeach()
# Distributions install the public headers under one or more of these
# prefixes (core headers in urma/ub, bonding headers next to them).
list(APPEND _URMA_LOCAL_INCLUDE_DIRS
     /usr/include/ub
     /usr/include/urma
     /usr/include/umdk
     /usr/include
     /usr/local/include
     /usr/local/include/urma
     /usr/local/include/umdk)

# Use private search variables and clear their cache entries. Otherwise a
# previous configure that fetched UMDK could hide a newly installed local SDK.
unset(_URMA_API_INCLUDE_DIR)
unset(_URMA_API_INCLUDE_DIR CACHE)
unset(_URMA_UBAGG_INCLUDE_DIR)
unset(_URMA_UBAGG_INCLUDE_DIR CACHE)

# urma_ubagg.h is required by the classic UB transport (urma_endpoint.h) and
# by TENT's urma_adapter, so a local install only counts as complete when both
# the core and the bonding headers are present.
find_path(
  _URMA_API_INCLUDE_DIR
  NAMES urma_api.h
  PATHS ${_URMA_LOCAL_INCLUDE_DIRS}
  NO_DEFAULT_PATH)
find_path(
  _URMA_UBAGG_INCLUDE_DIR
  NAMES urma_ubagg.h
  PATHS ${_URMA_LOCAL_INCLUDE_DIRS}
  NO_DEFAULT_PATH)

if (_URMA_API_INCLUDE_DIR AND _URMA_UBAGG_INCLUDE_DIR AND
    EXISTS "${_URMA_API_INCLUDE_DIR}/urma_api.h" AND
    EXISTS "${_URMA_UBAGG_INCLUDE_DIR}/urma_ubagg.h")
  set(urma_INCLUDE_DIR ${_URMA_API_INCLUDE_DIR} ${_URMA_UBAGG_INCLUDE_DIR})
  list(REMOVE_DUPLICATES urma_INCLUDE_DIR)
  message(STATUS "Using local URMA headers")
else()
  message(STATUS "Local URMA headers are incomplete; fetching UMDK")

  FetchContent_Declare(
    urma
    GIT_REPOSITORY https://atomgit.com/openeuler/umdk.git
    GIT_TAG master)
  FetchContent_MakeAvailable(urma)

  # UMDK stores the public headers in separate core and bond directories.
  unset(_URMA_API_INCLUDE_DIR CACHE)
  unset(_URMA_UBAGG_INCLUDE_DIR CACHE)
  find_path(
    _URMA_API_INCLUDE_DIR
    NAMES urma_api.h
    PATHS
      ${urma_SOURCE_DIR}/src/urma/lib/urma/core/include
    NO_DEFAULT_PATH)
  find_path(
    _URMA_UBAGG_INCLUDE_DIR
    NAMES urma_ubagg.h
    PATHS
      ${urma_SOURCE_DIR}/src/urma/lib/urma/bond/include
    NO_DEFAULT_PATH)

  if (NOT _URMA_API_INCLUDE_DIR OR
      NOT EXISTS "${_URMA_API_INCLUDE_DIR}/urma_api.h" OR
      NOT _URMA_UBAGG_INCLUDE_DIR OR
      NOT EXISTS "${_URMA_UBAGG_INCLUDE_DIR}/urma_ubagg.h")
    message(FATAL_ERROR
            "URMA headers not found after fetching UMDK: "
            "urma_api.h=${_URMA_API_INCLUDE_DIR}, "
            "urma_ubagg.h=${_URMA_UBAGG_INCLUDE_DIR}. Point URMA_ROOT/UMDK_ROOT "
            "at an installed UMDK SDK instead.")
  endif()

  set(urma_INCLUDE_DIR ${_URMA_API_INCLUDE_DIR} ${_URMA_UBAGG_INCLUDE_DIR})
  list(REMOVE_DUPLICATES urma_INCLUDE_DIR)
  message(STATUS "Using fetched UMDK source: ${urma_SOURCE_DIR}")
endif()

# Keep every name available for existing and conventional CMake callers.
# URMA_SYSTEM_INCLUDE_DIR is kept as an alias for callers/external scripts that
# used tent_merge's spelling.
set(URMA_SYSTEM_INCLUDE_DIR ${_URMA_API_INCLUDE_DIR})
set(URMA_API_INCLUDE_DIR ${_URMA_API_INCLUDE_DIR})
set(URMA_UBAGG_INCLUDE_DIR ${_URMA_UBAGG_INCLUDE_DIR})
set(URMA_INCLUDE_DIRS ${urma_INCLUDE_DIR})
message(STATUS "urma_INCLUDE_DIR: ${urma_INCLUDE_DIR}")

# Export the imported target used by main's TENT backends as well as the
# include variables used by the classic 930 UB transport.
find_library(URMA_LIBRARY NAMES urma
  HINTS ${URMA_ROOT} $ENV{URMA_ROOT} ${UMDK_ROOT} $ENV{UMDK_ROOT}
  PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64
  PATH_SUFFIXES lib lib64)
if(NOT TARGET Urma::urma)
  add_library(Urma::urma INTERFACE IMPORTED GLOBAL)
  set_property(TARGET Urma::urma PROPERTY INTERFACE_INCLUDE_DIRECTORIES "${urma_INCLUDE_DIR}")
  if(URMA_LIBRARY)
    set_property(TARGET Urma::urma PROPERTY INTERFACE_LINK_LIBRARIES "${URMA_LIBRARY}")
  endif()
endif()
set(URMA_INCLUDE_DIR "${urma_INCLUDE_DIR}")
set(URMA_FOUND TRUE)
if(URMA_LIBRARY)
  message(STATUS "URMA library: ${URMA_LIBRARY}")
else()
  message(STATUS "URMA library not found; real UB backends will be unavailable")
endif()
