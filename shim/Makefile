CC      ?= gcc
CFLAGS  ?= -O2 -Wall -Wextra -fPIC
LDFLAGS ?= -shared
LDLIBS  ?=

.PHONY: all clean
all: libmxl-intent.so

libmxl-intent.so: libmxl-intent.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f libmxl-intent.so
