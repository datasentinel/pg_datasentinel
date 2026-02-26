MODULE_big = test_pg_datasentinel_internals
OBJS = test_pg_datasentinel_internals.o ../dsdiag_utils.o

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