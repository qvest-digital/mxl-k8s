CC      ?= gcc
CFLAGS  ?= -O2 -Wall -Wextra -fPIC
LDFLAGS ?= -shared
LDLIBS  ?=

# The shim is LD_PRELOADed into consumer images this build never sees, so the
# build glibc is the floor of the supported consumer set rather than a local
# detail. The oldest target is EL8; a .so versioned above its 2.28 fails to
# load there, and the loader blames the consumer ("version GLIBC_x.y not found
# required by libmxl-intent.so") rather than the shim, so the check belongs
# here where the artefact is produced.
GLIBC_FLOOR ?= 2.28

.PHONY: all check clean
all: libmxl-intent.so check

libmxl-intent.so: libmxl-intent.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

# Fails when any versioned glibc symbol the .so requires is newer than the
# floor. Reaching libc through direct syscalls rather than dlsym is what keeps
# it low; a dlsym reintroduction lands at 2.34, where libdl merged into libc.
check: libmxl-intent.so
	@max=$$(objdump -T $< | grep -oE 'GLIBC_[0-9.]+' | sed 's/GLIBC_//' \
		| sort -uV | tail -1); \
	if [ -z "$$max" ]; then \
		echo "$<: no versioned glibc symbols found, check objdump" >&2; \
		exit 1; \
	fi; \
	if [ "$$(printf '%s\n%s\n' "$$max" "$(GLIBC_FLOOR)" | sort -V | tail -1)" \
		!= "$(GLIBC_FLOOR)" ]; then \
		echo "$<: requires GLIBC_$$max, above the $(GLIBC_FLOOR) floor;" \
			"consumers on older glibc cannot load it" >&2; \
		exit 1; \
	fi; \
	echo "$<: highest required glibc symbol $$max, floor $(GLIBC_FLOOR)"

clean:
	rm -f libmxl-intent.so
