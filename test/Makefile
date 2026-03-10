MODULE_big = test_pg_datasentinel_internals

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
OBJS = test_pg_datasentinel_internals.o ../pgds_utils.o ../linux/pgds_proc.o ../linux/pgds_cgroup.o
else
OBJS = test_pg_datasentinel_internals.o ../pgds_utils.o
endif

EXTENSION = test_pg_datasentinel_internals
DATA = test_pg_datasentinel_internals--1.0.sql

REGRESS = test_pg_datasentinel_internals

# Need to link against parent module's objects
#SHLIB_LINK = -L../ -lpg_datasentinel

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Add parent directory to include path
override CPPFLAGS := -I../ $(CPPFLAGS)