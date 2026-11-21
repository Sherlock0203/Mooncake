if(USE_UB)
  file(GLOB TENT_UB_SOURCES CONFIGURE_DEPENDS "*.cpp")
  add_library(tent_xport_ub STATIC ${TENT_UB_SOURCES})
  target_link_libraries(tent_xport_ub PUBLIC tent_common Urma::urma)
  if(URMA_LIBRARY)
    target_compile_definitions(tent_xport_ub PRIVATE TENT_HAS_REAL_URMA=1)
  endif()
  # D-22: BONDP bonding control is opt-in so physical-only builds compile and
  # link against the standard URMA headers/library alone. Without it, opening
  # a bonding device fails closed (NotImplemented) in RealUrmaAdapter.
  option(TENT_UB_BONDP
         "Enable BONDP bonding control (requires urma_ubagg.h and liburma)" OFF)
  if(TENT_UB_BONDP)
    if(NOT URMA_LIBRARY)
      message(FATAL_ERROR "TENT_UB_BONDP=ON requires URMA_LIBRARY (liburma)")
    endif()
    find_path(TENT_UB_BONDP_INCLUDE_DIR NAMES urma_ubagg.h
              HINTS "${URMA_INCLUDE_DIR}/../bond/include" /usr/include
                    /usr/local/include)
    if(NOT TENT_UB_BONDP_INCLUDE_DIR)
      message(FATAL_ERROR "TENT_UB_BONDP=ON but urma_ubagg.h was not found")
    endif()
    target_include_directories(tent_xport_ub
                               PRIVATE ${TENT_UB_BONDP_INCLUDE_DIR})
    target_compile_definitions(tent_xport_ub PRIVATE TENT_UB_HAS_BONDP=1)
  endif()
endif()
