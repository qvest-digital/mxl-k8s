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
 * /run/mxl/agent.sock). When any hooked call returns ENOENT for a
 * path matching ... .mxl-flow/flow_def.json, the shim connects to
 * the agent, sends `{"path":"<absolute path>"}\n`, waits for
 * `{"ok":true}\n` (or an error), and retries the original call.
 *
 * Outside that narrow path the hooks fall straight through to the
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
typedef int (*open_fn)(const char *pathname, int flags, ...);
typedef int (*access_fn)(const char *pathname, int mode);
typedef int (*stat_fn)(const char *pathname, struct stat *buf);
typedef int (*lstat_fn)(const char *pathname, struct stat *buf);

static openat_fn real_openat = NULL;
static open_fn   real_open   = NULL;
static access_fn real_access = NULL;
static stat_fn   real_stat   = NULL;
static lstat_fn  real_lstat  = NULL;

__attribute__((constructor)) static void shim_init(void)
{
	real_openat = (openat_fn)dlsym(RTLD_NEXT, "openat");
	real_open   = (open_fn)  dlsym(RTLD_NEXT, "open");
	real_access = (access_fn)dlsym(RTLD_NEXT, "access");
	real_stat   = (stat_fn)  dlsym(RTLD_NEXT, "stat");
	real_lstat  = (lstat_fn) dlsym(RTLD_NEXT, "lstat");
}

/* Return true when path is absolute and contains a non-empty
 * <id>.mxl-flow path component. libmxl probes the flow directory
 * itself (stat, access) and the access-file inside it before it
 * ever touches flow_def.json, so the shim cannot restrict its
 * trigger to that single filename. Matching at the directory-name
 * level keeps the shim narrow enough that unrelated opens
 * (/etc/..., /lib/..., locale data) still pass straight through.
 * No filesystem access -- pure string inspection. */
static bool is_flow_path(const char *path)
{
	if (!path || path[0] != '/') return false;

	size_t sfx = strlen(FLOW_SUFFIX);
	const char *p = path;
	while (*p) {
		while (*p == '/') p++;
		if (!*p) break;
		const char *start = p;
		while (*p && *p != '/') p++;
		size_t complen = (size_t)(p - start);
		if (complen > sfx &&
		    memcmp(p - sfx, FLOW_SUFFIX, sfx) == 0) {
			return true;
		}
	}
	return false;
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
	if (!is_flow_path(pathname)) {
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

/* libmxl resolves open(2) directly against libc rather than via
 * openat(2), so a separate hook is required. The flow-not-found
 * code path in libmxl reaches this entry before openat, which is
 * why hooking openat alone leaves the consumer's first open
 * attempt failing. */
int open(const char *pathname, int flags, ...)
{
	mode_t mode = 0;
	bool has_mode = (flags & O_CREAT) || (flags & __O_TMPFILE);
	if (has_mode) {
		va_list args;
		va_start(args, flags);
		mode = va_arg(args, mode_t);
		va_end(args);
	}

	if (!real_open) {
		errno = ENOSYS;
		return -1;
	}

	int fd = has_mode ? real_open(pathname, flags, mode)
			  : real_open(pathname, flags);
	if (fd >= 0 || errno != ENOENT) return fd;

	if (!is_flow_path(pathname)) {
		errno = ENOENT;
		return -1;
	}

	if (request_materialization(pathname) != 0) {
		errno = ENOENT;
		return -1;
	}

	return has_mode ? real_open(pathname, flags, mode)
			: real_open(pathname, flags);
}

/* libmxl probes the flow_def.json with access(F_OK) before
 * attempting to open it. Without this hook the probe returns
 * ENOENT and libmxl reports FLOW_NOT_FOUND without ever reaching
 * open or openat. */
int access(const char *pathname, int mode)
{
	if (!real_access) {
		errno = ENOSYS;
		return -1;
	}

	int rc = real_access(pathname, mode);
	if (rc == 0 || errno != ENOENT) return rc;

	if (!is_flow_path(pathname)) {
		errno = ENOENT;
		return -1;
	}

	if (request_materialization(pathname) != 0) {
		errno = ENOENT;
		return -1;
	}

	return real_access(pathname, mode);
}

/* libmxl also stat()s the flow_def.json during reader setup. Same
 * rationale as the access hook. */
int stat(const char *pathname, struct stat *buf)
{
	if (!real_stat) {
		errno = ENOSYS;
		return -1;
	}

	int rc = real_stat(pathname, buf);
	if (rc == 0 || errno != ENOENT) return rc;

	if (!is_flow_path(pathname)) {
		errno = ENOENT;
		return -1;
	}

	if (request_materialization(pathname) != 0) {
		errno = ENOENT;
		return -1;
	}

	return real_stat(pathname, buf);
}

int lstat(const char *pathname, struct stat *buf)
{
	if (!real_lstat) {
		errno = ENOSYS;
		return -1;
	}

	int rc = real_lstat(pathname, buf);
	if (rc == 0 || errno != ENOENT) return rc;

	if (!is_flow_path(pathname)) {
		errno = ENOENT;
		return -1;
	}

	if (request_materialization(pathname) != 0) {
		errno = ENOENT;
		return -1;
	}

	return real_lstat(pathname, buf);
}
