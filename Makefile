EXTENSION = datasentinel_diag

MODULE_big = datasentinel_diag
OBJS = datasentinel_diag.o

DATA = datasentinel_diag--0.1.0.sql

REGRESS = init
USE_PGXS = 1

ifdef USE_PGXS
PG_CONFIG := pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/datasentinel_diag
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
