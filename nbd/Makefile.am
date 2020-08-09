MAINTAINERCLEANFILES    = Makefile.in

AM_CPPFLAGS = -I$(top_builddir)/lib/shared -I/usr/include/libnl3

sbin_PROGRAMS = sbd-nbd

sbd_nbd_SOURCES = sbd-nbd.c

sbd_nbd_LDADD = -L$(top_builddir)/lib/.libs -lsheepdog -lnl-3 -lnl-genl-3

all-local:
	@echo Built sbd-nbd

clean-local:
	rm -f sbd-nbd *.o
