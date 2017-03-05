## Copyright (c) 2016, Robert Escriva
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##     * Redistributions of source code must retain the above copyright notice,
##       this list of conditions and the following disclaimer.
##     * Redistributions in binary form must reproduce the above copyright
##       notice, this list of conditions and the following disclaimer in the
##       documentation and/or other materials provided with the distribution.
##     * Neither the name of Consus nor the names of its contributors may be
##       used to endorse or promote products derived from this software without
##       specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
## IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
## ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
## LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
## CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
## SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
## INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
## CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
## POSSIBILITY OF SUCH DAMAGE.

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}

AM_CPPFLAGS  = -I${abs_top_srcdir}/include $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(REPLICANT_CFLAGS)
AM_CFLAGS    = $(WANAL_CFLAGS)
AM_CXXFLAGS  = $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(REPLICANT_CFLAGS) $(WANAL_CXXFLAGS)
AM_MAKEFLAGS = --no-print-directory
HELP2MAN_FLAGS = --no-discard-stderr --libtool --no-info --version-string=$(VERSION) --manual="Consus User Manual"
TESTS_ENVIRONMENT = . $(abs_top_srcdir)/test/env.sh "${abs_top_srcdir}" "${abs_top_builddir}" "${VERSION}";

BUILT_SOURCES =

EXTRA_DIST =
EXTRA_DIST += LICENSE
EXTRA_DIST += README

CLEANFILES =

lib_LTLIBRARIES = libpocdb.la

bin_PROGRAMS = pocdb-daemon pocdb-load

include_HEADERS = pocdb.h

noinst_HEADERS = common.h

libpocdb_la_SOURCES = client.cc
libpocdb_la_LIBADD =
libpocdb_la_LIBADD += $(BUSYBEE_LIBS)
libpocdb_la_LIBADD += $(E_LIBS)
libpocdb_la_LIBADD += $(PO6_LIBS)
libpocdb_la_LIBADD += -lpthread

pocdb_daemon_SOURCES = pocdb.cc
pocdb_daemon_LDADD =
pocdb_daemon_LDADD += $(BUSYBEE_LIBS)
pocdb_daemon_LDADD += $(E_LIBS)
pocdb_daemon_LDADD += $(PO6_LIBS)
pocdb_daemon_LDADD += -lleveldb
pocdb_daemon_LDADD += -lglog

pocdb_load_SOURCES = load.cc
pocdb_load_LDADD = libpocdb.la
