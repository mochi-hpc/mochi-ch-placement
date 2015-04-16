AC_PREREQ(2.50)
AC_INIT([ch-placement], [0.1],
        [carns@mcs.anl.gov],[],[])

AC_CONFIG_MACRO_DIR([m4])

AC_CONFIG_AUX_DIR([build-aux])

AM_INIT_AUTOMAKE([foreign subdir-objects -Wall])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_SRCDIR([include/ch-placement.h])
AC_CONFIG_HEADER([ch-placement-config.h])

AC_PROG_CC
AM_PROG_CC_C_O
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
LT_INIT([disable-shared])
AC_PROG_AWK
AC_PROG_LN_S
AS_MKDIR_P

AC_CHECK_SIZEOF([long int])

# We don't want to build shared libraries
#  not properly setup for it (versioning etc.)
AM_DISABLE_SHARED([true])

BUILD_ABSOLUTE_TOP=${PWD}
SRC_RELATIVE_TOP=$srcdir
SRC_ABSOLUTE_TOP=`cd $srcdir; pwd`

AC_SUBST(BUILD_ABSOLUTE_TOP)
AC_SUBST(SRC_RELATIVE_TOP)
AC_SUBST(SRC_ABSOLUTE_TOP)

AC_SUBST(CFLAGS)
AC_SUBST(LDFLAGS)
AC_SUBST(LIBS)
AC_SUBST(CPPFLAGS)

AC_CONFIG_FILES([Makefile maint/ch-placement.pc])

AC_OUTPUT