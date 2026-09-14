# Integration test dependency tracking
#
# These custom commands and targets express the dependency relationships between
# the compatibility test answer generators and the tests that consume them.
# The generate targets require Docker (Redis Stack) and must be run manually.

set(COMPAT_DIR ${CMAKE_SOURCE_DIR}/integration/compatibility)
set(INTEG_DIR ${CMAKE_SOURCE_DIR}/integration)

# Common source dependencies shared by all generators
set(COMPAT_COMMON_DEPS
    ${COMPAT_DIR}/__init__.py
    ${COMPAT_DIR}/data_sets.py
    ${COMPAT_DIR}/generate.py)

# Define a generator target: creates a custom command that produces the answers
# pickle from the given generator script, and wraps it in a named target.
function(add_answer_generator name generator_script)
    set(answers_file ${COMPAT_DIR}/${name}-answers.pickle.gz)

    add_custom_command(
        OUTPUT ${answers_file}
        COMMAND ${CMAKE_COMMAND} -E env
            pytest ${COMPAT_DIR}/${generator_script}
        DEPENDS
            ${COMPAT_DIR}/${generator_script}
            ${COMPAT_COMMON_DEPS}
            ${ARGN}
        WORKING_DIRECTORY ${COMPAT_DIR}
        COMMENT "Generating ${name}-answers.pickle.gz (requires Docker)"
    )

    add_custom_target(generate_${name}_answers
        DEPENDS ${answers_file})

    # Export the answers file path to the parent scope
    set(${name}_ANSWERS ${answers_file} PARENT_SCOPE)
endfunction()

add_answer_generator(filter    generate_filter.py)
add_answer_generator(aggregate generate.py)
add_answer_generator(text-search generate_text.py
    ${COMPAT_DIR}/text_query_builder.py)

# --- umbrella target for all answer generation ---

add_custom_target(generate_all_answers
    DEPENDS
        generate_filter_answers
        generate_aggregate_answers
        generate_text-search_answers)

# --- compatibility test target ---
# compatibility_test.py depends on all three answer files

add_custom_target(compatibility_test
    DEPENDS
        ${filter_ANSWERS}
        ${aggregate_ANSWERS}
        ${text-search_ANSWERS}
    SOURCES
        ${INTEG_DIR}/compatibility_test.py)
