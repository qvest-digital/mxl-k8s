/*
 * libmxl-intent.so: LD_PRELOAD shim that turns the ENOENT a libmxl
 * consumer hits on mxlCreateFlowReader(flowID) for a not-yet-
 * materialized flow into a synchronous wait until the agent has
 * arranged for the flow to appear locally.
 *
 * Build:
 *     gcc -fPIC -shared -O2 -Wall -Wextra \
 *         -o libmxl-intent.so libmxl-intent.c
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
 * The hooks reach the real libc through direct syscalls
 * (SYS_openat, SYS_faccessat, SYS_newfstatat) rather than
 * dlsym(RTLD_NEXT, ...), so the shim works against consumers built
 * on older glibc as well as current ones. On glibc 2.28,
 * dlsym(RTLD_NEXT, "open") can return NULL and turn every open call
 * into ENOSYS, and stat / lstat are not exposed as plain symbols at
 * all, only __xstat / __lxstat. Direct syscalls remove both failure
 * modes; the kernel ABI on x86_64 and aarch64 matches glibc's
 * struct stat.
 *
 * Outside the narrow flow_def.json path the hooks fall straight
 * through to the kernel.
 *
 * The hooks also report the reverse case. A successful open of a
 * path under <uuid>.mxl-flow with write intent and without O_CREAT
 * means a local producer attached to a flow that already existed,
 * which libmxl reaches via openFlow(..., READ_WRITE) and which
 * raises no filesystem event the agent can watch. The shim sends
 * `{"path":"<absolute path>","event":"attached"}\n` and does not
 * wait for the reply, so the producer's open is never delayed.
 */

#define _GNU_SOURCE
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
#include <sys/syscall.h>
#include <sys/un.h>
#include <unistd.h>

#define DEFAULT_SOCK_PATH "/run/mxl/agent.sock"
#define SOCK_ENV "MXL_INTENT_SOCK"
#define FLOW_SUFFIX ".mxl-flow"

static int sys_openat(int dirfd, const char *pathname, int flags, mode_t mode)
{
	return (int)syscall(SYS_openat, dirfd, pathname, flags, mode);
}

static int sys_access(const char *pathname, int mode)
{
	return (int)syscall(SYS_faccessat, AT_FDCWD, pathname, mode, 0);
}

static int sys_stat(const char *pathname, struct stat *buf)
{
	return (int)syscall(SYS_newfstatat, AT_FDCWD, pathname, buf, 0);
}

static int sys_lstat(const char *pathname, struct stat *buf)
{
	return (int)syscall(SYS_newfstatat, AT_FDCWD, pathname, buf,
			    AT_SYMLINK_NOFOLLOW);
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

/* Send one line-delimited JSON request to the agent. When
 * want_reply is false the caller is not blocked on an answer: the
 * request is written and the connection dropped. Returns 0 when the
 * agent answered {"ok":true}, or when want_reply is false and the
 * write succeeded; -1 on any failure including timeout. */
static int agent_send(const char *req, int n, bool want_reply)
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

	if (!want_reply) {
		close(fd);
		return 0;
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

/* Talk to the agent over the UDS. Returns 0 on success (the agent
 * confirmed the flow is, or will be, available), -1 on any failure
 * including timeout. */
static int request_materialization(const char *path)
{
	char req[PATH_MAX + 32];
	int n = snprintf(req, sizeof(req), "{\"path\":\"%s\"}\n", path);
	if (n < 0 || (size_t)n >= sizeof(req)) return -1;
	return agent_send(req, n, true);
}

/* Copy the <id>.mxl-flow component of path into out. Returns false
 * when path carries no such component. */
static bool flow_component(const char *path, char *out, size_t outlen)
{
	size_t sfx = strlen(FLOW_SUFFIX);
	const char *p = path;
	while (*p) {
		while (*p == '/') p++;
		if (!*p) break;
		const char *start = p;
		while (*p && *p != '/') p++;
		size_t complen = (size_t)(p - start);
		if (complen > sfx && memcmp(p - sfx, FLOW_SUFFIX, sfx) == 0) {
			if (complen >= outlen) return false;
			memcpy(out, start, complen);
			out[complen] = '\0';
			return true;
		}
	}
	return false;
}

/* Flows already announced by this process. Opening an existing flow
 * for writing touches the flow data file and then every grain file
 * in the ring -- a hundred or more on a long-history flow -- and all
 * of them sit under the same <id>.mxl-flow directory. Announcing
 * each one would turn one event into a hundred.
 *
 * Deliberately racy: two threads attaching to the same flow at once
 * can both announce it. The agent's handler is idempotent, so a
 * duplicate costs one extra round trip and nothing else, which is
 * cheaper than carrying a lock on a path that runs at flow-open
 * frequency. The table is a ring; overflow re-announces, it does not
 * corrupt. */
#define SEEN_MAX 64
#define SEEN_ENTRY_MAX 80
static char seen_flows[SEEN_MAX][SEEN_ENTRY_MAX];
static unsigned seen_next;

static bool already_announced(const char *flow)
{
	for (unsigned i = 0; i < SEEN_MAX; i++) {
		if (seen_flows[i][0] == '\0') continue;
		if (strcmp(seen_flows[i], flow) == 0) return true;
	}
	unsigned slot = seen_next++ % SEEN_MAX;
	size_t n = strlen(flow);
	if (n < SEEN_ENTRY_MAX) memcpy(seen_flows[slot], flow, n + 1);
	return false;
}

/* Tell the agent a local process just attached to a flow that
 * already existed on this node.
 *
 * libmxl builds a new flow in a temporary directory and renames it
 * into place, so a producer that creates its flow raises a rename
 * the agent's fanotify watch already sees. A producer that finds the
 * directory in place raises nothing at all: mxlCreateFlowWriter
 * falls through to openFlow(..., READ_WRITE) and the agent never
 * learns the node gained a producer. That is the gap this closes,
 * and it is the only positive evidence available that a local
 * process -- rather than a mirror the gateway is filling -- owns the
 * flow.
 *
 * Best effort by construction: the caller's open has already
 * succeeded, so a failure here must not change what it returns. */
static void notify_attached(const char *path)
{
	char flow[SEEN_ENTRY_MAX];
	if (!flow_component(path, flow, sizeof(flow))) return;
	if (already_announced(flow)) return;

	char req[PATH_MAX + 64];
	int n = snprintf(req, sizeof(req),
		"{\"path\":\"%s\",\"event\":\"attached\"}\n", path);
	if (n < 0 || (size_t)n >= sizeof(req)) return;
	(void)agent_send(req, n, false);
}

/* True when flags describe attaching to something that already
 * exists, with the intent to write it. O_CREAT is excluded because
 * libmxl only passes it while building a flow in its temporary
 * directory, which the rename already reports. */
static bool is_write_attach(int flags)
{
	if (flags & O_CREAT) return false;
	int acc = flags & O_ACCMODE;
	return acc == O_RDWR || acc == O_WRONLY;
}

int openat(int dirfd, const char *pathname, int flags, ...)
{
	mode_t mode = 0;
	bool has_mode = (flags & O_CREAT) || (flags & __O_TMPFILE);
	if (has_mode) {
		va_list args;
		va_start(args, flags);
		mode = (mode_t)va_arg(args, int);
		va_end(args);
	}

	int fd = sys_openat(dirfd, pathname, flags, mode);
	if (fd >= 0) {
		if (is_write_attach(flags) && is_flow_path(pathname)) {
			int saved = errno;
			notify_attached(pathname);
			errno = saved;
		}
		return fd;
	}
	if (errno != ENOENT) return fd;

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

	return sys_openat(dirfd, pathname, flags, mode);
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
		mode = (mode_t)va_arg(args, int);
		va_end(args);
	}

	int fd = sys_openat(AT_FDCWD, pathname, flags, mode);
	if (fd >= 0) {
		if (is_write_attach(flags) && is_flow_path(pathname)) {
			int saved = errno;
			notify_attached(pathname);
			errno = saved;
		}
		return fd;
	}
	if (errno != ENOENT) return fd;

	if (!is_flow_path(pathname)) {
		errno = ENOENT;
		return -1;
	}

	if (request_materialization(pathname) != 0) {
		errno = ENOENT;
		return -1;
	}

	return sys_openat(AT_FDCWD, pathname, flags, mode);
}

/* libmxl probes the flow_def.json with access(F_OK) before
 * attempting to open it. Without this hook the probe returns
 * ENOENT and libmxl reports FLOW_NOT_FOUND without ever reaching
 * open or openat. */
int access(const char *pathname, int mode)
{
	int rc = sys_access(pathname, mode);
	if (rc == 0 || errno != ENOENT) return rc;

	if (!is_flow_path(pathname)) {
		errno = ENOENT;
		return -1;
	}

	if (request_materialization(pathname) != 0) {
		errno = ENOENT;
		return -1;
	}

	return sys_access(pathname, mode);
}

/* libmxl also stat()s the flow_def.json during reader setup. Same
 * rationale as the access hook. On glibc < 2.33 the consumer
 * binary reaches stat via inline __xstat and our `stat` symbol is
 * never called; the hook still exists for glibc 2.33+ consumers
 * that link to plain `stat`. */
int stat(const char *pathname, struct stat *buf)
{
	int rc = sys_stat(pathname, buf);
	if (rc == 0 || errno != ENOENT) return rc;

	if (!is_flow_path(pathname)) {
		errno = ENOENT;
		return -1;
	}

	if (request_materialization(pathname) != 0) {
		errno = ENOENT;
		return -1;
	}

	return sys_stat(pathname, buf);
}

int lstat(const char *pathname, struct stat *buf)
{
	int rc = sys_lstat(pathname, buf);
	if (rc == 0 || errno != ENOENT) return rc;

	if (!is_flow_path(pathname)) {
		errno = ENOENT;
		return -1;
	}

	if (request_materialization(pathname) != 0) {
		errno = ENOENT;
		return -1;
	}

	return sys_lstat(pathname, buf);
}
