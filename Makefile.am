ACLOCAL_AMFLAGS = -I m4

AUTOMAKE_OPTIONS = parallel-tests

TESTS =
CLEANFILES =

noinst_HEADERS =
check_PROGRAMS =
bin_PROGRAMS =
noinst_LTLIBRARIES =
lib_LTLIBRARIES =
include_HEADERS = include/ch-placement.h include/ch-placement-crush.h include/ch-placement-oid-gen.h

AM_CPPFLAGS =

EXTRA_DIST = COPYRIGHT prepare
nobase_include_HEADERS=

lib_LTLIBRARIES += lib/libch-placement.la
lib_libch_placement_la_SOURCES = 
lib_libch_placement_la_LIBADD =

LDADD = lib/libch-placement.la -lm

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/ch-placement.pc

AM_CPPFLAGS += -I$(top_srcdir)/include

include src/Makefile.subdir
include src/modules/Makefile.subdir
include tests/Makefile.subdir

