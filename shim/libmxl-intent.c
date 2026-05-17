/*
 * libmxl-intent.so: LD_PRELOAD shim that turns the ENOENT a libmxl
 * consumer hits on mxlCreateFlowReader(flowID) for a not-yet-
 * materialized flow into a synchronous wait until the agent has
 * arranged for the flow to appear locally.
 *
 * Build:
 *     gcc -fPIC -shared -O2 -Wall -Wextra \
 *         -o libmxl-intent.so libmxl-intent.c -ldl
 *
 * Use:
 *     LD_PRELOAD=/path/to/libmxl-intent.so /usr/local/bin/your-app
 *
 * Configure the agent socket via $MXL_INTENT_SOCK (default
 * /run/mxl/agent.sock). When the hook sees an openat() return
 * ENOENT for a path matching ... .mxl-flow/flow_def.json, it
 * connects to the agent, sends `{"path":"<absolute path>"}\n`,
 * waits for `{"ok":true}\n` (or an error), and retries the open.
 *
 * Outside that narrow path, openat() falls straight through to the
 * real glibc implementation.
 */

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#define DEFAULT_SOCK_PATH "/run/mxl/agent.sock"
#define SOCK_ENV "MXL_INTENT_SOCK"
#define FLOW_SUFFIX ".mxl-flow"
#define DEF_FILENAME "flow_def.json"

typedef int (*openat_fn)(int dirfd, const char *pathname, int flags, ...);
static openat_fn real_openat = NULL;

__attribute__((constructor)) static void shim_init(void)
{
	real_openat = (openat_fn)dlsym(RTLD_NEXT, "openat");
}

/* Return true when path looks like
 * .../<something><FLOW_SUFFIX>/flow_def.json.
 * No filesystem access — pure string inspection. */
static bool is_flow_def_path(const char *path)
{
	if (!path || path[0] != '/') return false;

	size_t len = strlen(path);
	size_t fn = strlen(DEF_FILENAME);
	if (len <= fn + 1) return false;
	if (memcmp(path + len - fn, DEF_FILENAME, fn) != 0) return false;
	if (path[len - fn - 1] != '/') return false;

	const char *parent_end = path + len - fn - 1;
	size_t sfx = strlen(FLOW_SUFFIX);
	if ((size_t)(parent_end - path) < sfx) return false;
	if (memcmp(parent_end - sfx, FLOW_SUFFIX, sfx) != 0) return false;

	/* Require at least one character before .mxl-flow (the flow id). */
	const char *parent_start = parent_end - sfx - 1;
	while (parent_start >= path && *parent_start != '/') parent_start--;
	if (parent_end - sfx <= parent_start + 1) return false;
	return true;
}

/* Talk to the agent over the UDS. Returns 0 on success (the agent
 * confirmed the flow is, or will be, available), -1 on any failure
 * including timeout. */
static int request_materialization(const char *path)
{
	const char *sock_path = getenv(SOCK_ENV);
	if (!sock_path || !*sock_path) sock_path = DEFAULT_SOCK_PATH;

	int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (fd < 0) return -1;

	struct sockaddr_un addr;
	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	if (strlen(sock_path) >= sizeof(addr.sun_path)) {
		close(fd);
		return -1;
	}
	strcpy(addr.sun_path, sock_path);

	if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		close(fd);
		return -1;
	}

	char req[PATH_MAX + 32];
	int n = snprintf(req, sizeof(req), "{\"path\":\"%s\"}\n", path);
	if (n < 0 || (size_t)n >= sizeof(req)) {
		close(fd);
		return -1;
	}

	ssize_t written = 0;
	while (written < n) {
		ssize_t w = write(fd, req + written, n - written);
		if (w < 0) {
			if (errno == EINTR) continue;
			close(fd);
			return -1;
		}
		written += w;
	}

	char resp[1024];
	size_t r_off = 0;
	while (r_off < sizeof(resp) - 1) {
		ssize_t r = read(fd, resp + r_off, sizeof(resp) - 1 - r_off);
		if (r < 0) {
			if (errno == EINTR) continue;
			close(fd);
			return -1;
		}
		if (r == 0) break;
		r_off += r;
		if (memchr(resp + r_off - r, '\n', r) != NULL) break;
	}
	close(fd);
	resp[r_off] = '\0';

	/* Bare substring check; the agent always emits {"ok":true} or
	 * {"ok":false,"error":...} so this is unambiguous. */
	return (strstr(resp, "\"ok\":true") != NULL) ? 0 : -1;
}

int openat(int dirfd, const char *pathname, int flags, ...)
{
	mode_t mode = 0;
	bool has_mode = (flags & O_CREAT) || (flags & __O_TMPFILE);
	if (has_mode) {
		va_list args;
		va_start(args, flags);
		mode = va_arg(args, mode_t);
		va_end(args);
	}

	if (!real_openat) {
		errno = ENOSYS;
		return -1;
	}

	int fd = has_mode ? real_openat(dirfd, pathname, flags, mode)
			  : real_openat(dirfd, pathname, flags);
	if (fd >= 0 || errno != ENOENT) return fd;

	/* Only intervene for absolute flow_def.json paths. The shim is
	 * intentionally narrow so unrelated opens (libc, libpthread,
	 * /etc/...) pass straight through. */
	if (!is_flow_def_path(pathname)) {
		errno = ENOENT;
		return -1;
	}

	if (request_materialization(pathname) != 0) {
		errno = ENOENT;
		return -1;
	}

	return has_mode ? real_openat(dirfd, pathname, flags, mode)
			: real_openat(dirfd, pathname, flags);
}
