MODULE_big = test_datasentinel_diag_internals
OBJS = test_datasentinel_diag_internals.o ../dsdiag_utils.o

EXTENSION = test_datasentinel_diag_internals
DATA = test_datasentinel_diag_internals--1.0.sql

REGRESS = test_datasentinel_diag_internals

# Need to link against parent module's objects
#SHLIB_LINK = -L../ -ldatasentinel_diag

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Add parent directory to include path
override CPPFLAGS := -I../ $(CPPFLAGS)