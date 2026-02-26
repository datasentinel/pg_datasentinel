EXTENSION = pg_datasentinel

MODULE_big = pg_datasentinel
OBJS = pg_datasentinel.o dsdiag_linux.o dsdiag_utils.o

DATA = pg_datasentinel--0.1.0.sql

REGRESS = init autovacuum
REGRESS_OPTS = --temp-config=regress.conf
USE_PGXS = 1

ifdef USE_PGXS
PG_CONFIG := pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_datasentinel
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
