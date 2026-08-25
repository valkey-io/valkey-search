# allows creation of interface object library which "acts" like
# a normal interface target using target_sources
#
# this is necessary when an object library collects other objects, because
# cmake does not allow OBJECT libraries to act like interfaces recursively.


function (create_object_interface name)
    add_library(${name} INTERFACE)
    foreach (src ${ARGN})
        target_link_libraries(${name} INTERFACE ${src})
        target_sources(${name} INTERFACE $<TARGET_OBJECTS:${src}>)
    endforeach()
endfunction()

# incorporate this target's objects.. only used if target
# is actually an object
function (link_target_objects name)
    foreach (lib ${ARGN})
        if(TARGET ${lib})
            get_target_property(_type ${lib} TYPE)
            if(_type STREQUAL "OBJECT_LIBRARY")
                target_sources(search_base PUBLIC $<TARGET_OBJECTS:${lib}>)
            endif()
        endif()
    endforeach ()
endfunction()