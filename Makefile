EXTENSION = pg_datasentinel

MODULE_big = pg_datasentinel

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
OBJS = pg_datasentinel.o linux/pgds_proc.o linux/pgds_cgroup.o pgds_utils.o
else
OBJS = pg_datasentinel.o pgds_utils.o
endif

DATA = pg_datasentinel--1.0.sql

PG_VERSION := $(shell pg_config --version | sed "s/^PostgreSQL //" | sed "s/\.[0-9]*$$//")
PG_VERSION := $(shell [ "$(PG_VERSION)" -lt 18 ] 2>/dev/null && echo "before_18" || echo "$(PG_VERSION)")

REGRESS = init vacuum analyze tempfiles checkpoints wraparound manualanalyze_$(PG_VERSION) excludeInternalSchemas_$(PG_VERSION) fakelog
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
