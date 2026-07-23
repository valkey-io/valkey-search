set(SRCS_HDRHISTOGRAM_C
    "hdr_encoding.c"
    "hdr_histogram.c"
    "hdr_interval_recorder.c"
    "hdr_thread.c"
    "hdr_time.c"
    "hdr_writer_reader_phaser.c"
    "hdr_atomic.h"
    "hdr_encoding.h"
    "hdr_endian.h"
    "hdr_histogram.h"
    "hdr_interval_recorder.h"
    "hdr_tests.h"
    "hdr_thread.h"
    "hdr_time.h"
    "hdr_writer_reader_phaser.h")

add_library(hdrhistogram_c OBJECT ${SRCS_HDRHISTOGRAM_C})
target_include_directories(hdrhistogram_c PUBLIC .)