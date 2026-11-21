/* SPDX-License-Identifier: GPL-2.0-only WITH Linux-syscall-note */
/* mxlfs userspace interface. */
#ifndef MXLFS_UAPI_H
#define MXLFS_UAPI_H

#include <linux/ioctl.h>
#include <linux/types.h>

#define MXLFS_MAGIC 0x6d786c66 /* "mxlf" */

#define MXLFS_VIEW_MAX_SOURCES 8

/*
 * The largest view. A view keeps a table of one byte per page, 1 MiB at
 * this size with 4 KiB pages, charged to the memory cgroup of the process
 * that makes the view.
 */
#define MXLFS_VIEW_MAX_SIZE (4ULL << 30)

/*
 * Turns an empty regular file into a view: each of its pages is the page at
 * the same offset of one of `count` source files on the same mount, all of
 * the view's size, each fd open for reading (EBADF otherwise). The view must
 * have no pages and no mappings of its own and be neither a view nor a
 * source of one already (EBUSY otherwise). A view of size 0 or larger than
 * MXLFS_VIEW_MAX_SIZE fails with EINVAL, as does a source of another size,
 * also when the view is resized during the call, a source that is not a
 * regular file on the same mount, is the file itself or is a view, a count
 * of 0 or above MXLFS_VIEW_MAX_SOURCES, and an entry of fds past count
 * other than 0: unused entries are 0. EFAULT when the argument cannot be
 * read, ENOMEM when the view or its selection table cannot be allocated.
 * Every page starts out selecting source 0. While a
 * view uses a file, truncate, fallocate and write cannot change its size
 * (EBUSY), and truncate cannot change the view's own size (EBUSY).
 *
 * The errnos split by what a call needs. The view ioctls (SELECT,
 * SELECT_RANGE, GET_SELECTED, REPLACE_SOURCE) fail with EINVAL on a file
 * that is not a view, as with any argument they do not accept. A view has
 * no pages of its own, so write, fallocate, the ioctls that allocate or
 * place pages (MXLFS_IOC_ALLOCATE, MXLFS_IOC_SET_NODE, MXLFS_IOC_MOVE) and
 * MXLFS_IOC_HOLD fail on it with EPERM, as for a file whose contents may
 * not change.
 *
 * flags is checked before anything else, on any regular file: unknown
 * bits fail with EINVAL, so flags = ~0 probes for the ioctl as for
 * MXLFS_IOC_VIEW_REPLACE_SOURCE; a directory answers ENOTTY. Then EBADF if
 * the file is not open for writing.
 */
struct mxlfs_view_sources {
	__u32 count;
	__u32 flags;
	__s32 fds[MXLFS_VIEW_MAX_SOURCES];
};

/*
 * MXLFS_IOC_STATS takes the size of the caller's struct from the command
 * and copies that much of this one, or all of it and zeroes the rest if the
 * caller's is larger. Before view_replaces the struct was 32 bytes,
 * _IOR('x', 4, __u64[4]); callers built against that header still work,
 * and a size below it fails with EINVAL, not ENOTTY as a size the module
 * does not match would.
 */
#define MXLFS_STATS_SIZE_VER0 32	/* the struct before view_replaces */

struct mxlfs_stats {
	__u64 bytes_allocated;
	__u64 bytes_limit;
	__u64 view_selects;	/* SELECT calls that changed the source */
	__u64 view_faults;	/* faults on views; each maps one folio */
	__u64 view_replaces;	/* REPLACE_SOURCE calls that changed a source */
	/* Folios that preferred a node (numa_node= or the file's) and got another. */
	__u64 numa_fallback_folios;
	__u64 numa_fallback_bytes;
	/* MXLFS_IOC_MOVE, since the mount; dry runs count only in dry_runs. */
	__u64 move_calls;
	__u64 move_calls_partial;	/* result MXLFS_MOVED_PARTIAL */
	__u64 move_calls_none;		/* blocks were off the node, none moved */
	__u64 move_blocks;		/* folios moved */
	__u64 move_bytes;
	__u64 move_ns;			/* sum of call durations */
	__u64 move_refused[8];		/* blocks left off the node, by MXLFS_REFUSE_* */
	/*
	 * Block windows, one per folio moved: from the last check of the
	 * folio for writes and its unmap until its replacement is unlocked,
	 * which is the longest a fault on it waits. Histogram
	 * buckets: <64 us, <128 us, <256 us, <512 us, <1 ms, <2 ms, <4 ms,
	 * <8 ms, >=8 ms.
	 */
	__u64 move_window_ns_sum;
	__u64 move_window_ns_max;
	__u64 move_window_hist[9];
	__u64 holds;			/* open files holding a file now (MXLFS_IOC_HOLD) */
	__u64 dry_runs;
	__u64 wanted;		/* missing flows walks wait for now */
	/* Walks that found a flow missing and did not wait: the caps were full. */
	__u64 wanted_refused;
};

/*
 * A missing flow some path walk is waiting for (wait= mount option); a walk
 * with RESOLVE_NO_XDEV never waits. On a directory fd of the mount (ENOTTY
 * on another file), with CAP_SYS_ADMIN (EPERM otherwise, checked next):
 * MXLFS_IOC_WANTED_NEXT returns the next flow not yet reported, waiting up
 * to timeout_ms; ETIMEDOUT if none. flags is checked right after the
 * capability: any bit set fails with EINVAL, so a later flag can be
 * probed. flags and timeout_ms are written back as passed, so the struct
 * can be passed again for the next flow. A flow counts as reported once
 * the struct is written back: EFAULT there leaves it to the next call, and
 * the struct's contents are then undefined.
 *
 * path holds at most 4075 bytes and its NUL. A walk through a missing
 * flow whose path from the filesystem root is longer fails with
 * ENAMETOOLONG at once instead of waiting, unless the mount's or its
 * thread group's cap on waits is full, when it fails with ENOENT at once;
 * WANTED_NEXT never has a path it cannot hold.
 *
 * Creating the flow wakes its waiters; MXLFS_IOC_WANTED_FAIL fails them
 * with the given errno instead, which must be from 1 to 511, with pad 0
 * (EINVAL otherwise; ENOENT for an id nobody waits for). 512 and up are
 * the kernel's own codes, never returned to userspace.
 */
struct mxlfs_wanted_req {
	__u64 id;
	__u32 pid;		/* first waiter's tgid in the caller's pid namespace, or 0 */
	__u32 timeout_ms;	/* in */
	char path[4076];	/* from the filesystem root, e.g. /domain/<id>.mxl-flow */
	__u32 flags;		/* in, 0 */
};

struct mxlfs_wanted_fail {
	__u64 id;
	__s32 error;
	__u32 pad;		/* 0 */
};

/*
 * Selects source for the pages of [offset, offset + length): offset must be
 * page aligned, the end is rounded up to a page and clipped to the view.
 * EINVAL for flags other than 0, a length of 0, and an offset that is not
 * page aligned or not inside the view.
 */
struct mxlfs_view_range {
	__u32 source;
	__u32 flags;
	__u64 offset;
	__u64 length;
};

#define MXLFS_IOC_VIEW_SET_SOURCES _IOW('x', 1, struct mxlfs_view_sources)
/*
 * SELECT, SELECT_RANGE and GET_SELECTED fail with EINVAL on a file that
 * is not a view and with ENOTTY on a directory, EFAULT when the argument
 * cannot be read or written. SELECT and SELECT_RANGE need the view open
 * for writing (EBADF otherwise) and fail with EINVAL for a source of
 * count or above. Selecting the source the whole range selects already
 * changes and zaps nothing. GET_SELECTED needs no write access.
 */
#define MXLFS_IOC_VIEW_SELECT _IOW('x', 2, __u32)	/* the whole view */
#define MXLFS_IOC_VIEW_GET_SELECTED _IOR('x', 3, __u32)	/* of the first page */
#define MXLFS_IOC_STATS _IOR('x', 4, struct mxlfs_stats)
#define MXLFS_IOC_WANTED_NEXT _IOWR('x', 5, struct mxlfs_wanted_req)
#define MXLFS_IOC_WANTED_FAIL _IOW('x', 6, struct mxlfs_wanted_fail)

/*
 * With 1, the calling thread group's fallocate calls on this mount only
 * set the file size; pages are allocated on first touch. 0 turns it off;
 * other values fail with EINVAL. Needs no privilege.
 */
#define MXLFS_IOC_LAZY_ALLOC _IOW('x', 7, __u32)

/*
 * On a file open for writing: allocates all of it in large folios, folding
 * pages already there in, whether or not the caller allocates lazily.
 * EBADF if the file is not open for writing, EPERM on a view. It is
 * fallocate(FALLOC_FL_KEEP_SIZE) over the file in every other respect:
 * the same permission checks and errors, and a modify event on success.
 * The one difference: on an empty file other than a view it does nothing
 * and returns 0, where fallocate fails a zero length with EINVAL.
 */
#define MXLFS_IOC_ALLOCATE _IO('x', 8)
/* Errors as for MXLFS_IOC_VIEW_SELECT, and those of struct mxlfs_view_range. */
#define MXLFS_IOC_VIEW_SELECT_RANGE _IOW('x', 9, struct mxlfs_view_range)

/*
 * On a view open for writing: makes the file at fd the view's source number
 * `source` in place of the one there, under the same conditions
 * MXLFS_IOC_VIEW_SET_SOURCES puts on a source. Mappings of the pages that
 * select it are zapped and refault onto the new file's pages; readers keep
 * their mappings. The selection stays as it is. The file already there
 * leaves the view unchanged.
 *
 * flags is checked before anything else, on any regular file: unknown
 * bits fail with EINVAL. With flags = ~0, EINVAL means the module has this
 * ioctl and ENOTTY that it does not; a directory answers ENOTTY. Then
 * EINVAL if the file is not a view, EBADF if it is not open for writing,
 * EINVAL for a source number out of range, EBADF if fd is not open for
 * reading, and EINVAL if the file at fd cannot be a source of the view.
 */
struct mxlfs_view_replace {
	__u32 source;
	__s32 fd;
	__u32 flags;
};

#define MXLFS_IOC_VIEW_REPLACE_SOURCE _IOW('x', 10, struct mxlfs_view_replace)

/*
 * NUMA placement of a file's pages. A folio of a regular file comes from
 * the file's preferred node if it has one with memory, else from the
 * mount's numa_node=, else from where the allocating task's memory policy
 * puts it. A preferred node falls back to the others when it has no free
 * memory, and the fallback is counted. Without these calls a file follows
 * numa_node= alone.
 */
#define MXLFS_NODE_NONE (-1)

/*
 * On a regular file that is not a view, with CAP_SYS_NICE: later
 * allocations of the file prefer node; MXLFS_NODE_NONE clears the
 * preference. Pages already there stay where they are. EINVAL for a node
 * without memory or flags other than 0, EPERM on a view, EBUSY while
 * another open file holds the file (MXLFS_IOC_HOLD).
 */
struct mxlfs_node_pref {
	__s32 node;
	__u32 flags;
};

#define MXLFS_IOC_SET_NODE _IOW('x', 11, struct mxlfs_node_pref)

/*
 * Where a file's pages are, on any open regular file, without privilege
 * and without taking a lock: pages_ptr receives the file's pages per node
 * for the first nr_nodes nodes, and nr_nodes is set to the number of node
 * ids. A page counts while it is in the page cache and uptodate. With
 * MXLFS_NODE_PAGES_RECOUNT (CAP_SYS_ADMIN) the pages are counted by
 * walking the file's page cache under its locks instead; that blocks the
 * file's writers and is meant for tests and diagnosis. EINVAL for
 * unknown flags or a pad other than 0.
 */
struct mxlfs_node_pages {
	__u64 pages_ptr;	/* in: __u64[nr_nodes] */
	__u32 nr_nodes;		/* in: entries at pages_ptr; out: number of node ids */
	__u32 flags;		/* in: MXLFS_NODE_PAGES_* */
	__s32 pref;		/* out: the file's preferred node or MXLFS_NODE_NONE */
	__s32 effective;	/* out: the node allocations prefer, or MXLFS_NODE_NONE */
	__u32 holds;		/* out: open files holding the file */
	__u32 pad;		/* in and out: 0 */
};

#define MXLFS_NODE_PAGES_RECOUNT (1U << 0)
#define MXLFS_IOC_NODE_PAGES _IOWR('x', 12, struct mxlfs_node_pages)

/*
 * On a regular file that is not a view, with CAP_SYS_NICE (none for a dry
 * run): moves every folio of the file that is not on node to node, one at
 * a time, keeping offsets and contents; mappings of a folio refault onto
 * its copy. A destination folio of the same size is reserved on node for
 * every folio first; if any reservation fails nothing moves (ENOMEM). A
 * folio is left where it is if it is pinned (long-term GUP, such as an
 * RDMA registration), mapped by a view, holds references beyond its
 * mappings and the page cache, or holds a page with a memory error, which
 * stays in the file; the first such folio sets reason and refused_offset.
 * Without MXLFS_MOVE_STRICT the call goes on with the next folio and
 * returns 0, result MXLFS_MOVED_PARTIAL or MXLFS_MOVED_NONE. With it, every
 * folio is checked for a pin, a memory error or a view's mapping before
 * any moves, which zaps its mappings once more, and such a folio fails the
 * call with EBUSY; one refused later fails it with
 * EAGAIN, the folios moved so far staying moved. EBUSY with reason
 * MXLFS_REFUSE_HELD when another open file holds the file and a folio is
 * off node; EINTR on a fatal signal, between folios. Each folio is copied
 * before its window; it holds up a fault on it for that window only, from
 * the zap until the copy is in place, and a folio written to while it was
 * copied is tried again after the others, the last try copying it within the
 * window; the whole call holds the file's inode lock, so write(), fallocate
 * and truncation wait for it. The file's preferred node becomes node when
 * the result is MXLFS_MOVED_ALL and no other open file holds it.
 * MXLFS_MOVE_DRY_RUN changes nothing and takes no lock: it counts and
 * reports what a move would find before it starts, pins, memory errors
 * and holds, not a view's mappings. EINVAL for unknown flags or a node
 * without memory, EPERM on a view, also for a dry run.
 */
struct mxlfs_move {
	__s32 node;		/* in */
	__u32 flags;		/* in: MXLFS_MOVE_* */
	__u32 result;		/* out: MXLFS_MOVED_* */
	/* out: MXLFS_REFUSE_* that stopped the call, else of the first folio left, or 0 */
	__u32 reason;
	__u64 blocks;		/* out: folios of the file */
	__u64 blocks_on_node;	/* out: on node before the call */
	__u64 blocks_moved;	/* out */
	__u64 blocks_refused;	/* out: left off node */
	__u64 bytes_moved;	/* out */
	__u64 refused_offset;	/* out: byte offset of the folio reason belongs to */
	__u64 duration_ns;	/* out: the whole call */
	__u64 max_window_ns;	/* out: the longest block window, as in mxlfs_stats */
};

#define MXLFS_MOVE_DRY_RUN (1U << 0)
#define MXLFS_MOVE_STRICT (1U << 1)

enum {
	MXLFS_MOVED_ALL = 1,	/* every folio is on node now */
	MXLFS_MOVED_PARTIAL,
	MXLFS_MOVED_NONE,	/* folios were off node, none moved */
};

enum {
	MXLFS_REFUSE_PINNED = 1,	/* long-term pinned */
	MXLFS_REFUSE_VIEW,		/* mapped by a view */
	MXLFS_REFUSE_BUSY,		/* references beyond mappings and page cache */
	MXLFS_REFUSE_NOMEM,		/* no destination folio could be reserved */
	MXLFS_REFUSE_HELD,		/* another open file holds the file */
	MXLFS_REFUSE_SIGNAL,		/* a fatal signal stopped the call */
	MXLFS_REFUSE_POISONED,		/* holds a page with a memory error */
	MXLFS_REFUSE_MAX = 8
};

#define MXLFS_IOC_MOVE _IOWR('x', 13, struct mxlfs_move)

/*
 * 1 takes, 0 drops this open file's hold on its file, with CAP_SYS_NICE.
 * While another open file holds a file, MXLFS_IOC_MOVE and
 * MXLFS_IOC_SET_NODE through this one fail with EBUSY. A hold belongs to
 * the open file, so dup() and fork() share it, and it ends with the
 * file's last close, as when its process dies. EINVAL for other values,
 * EPERM on a view.
 */
#define MXLFS_IOC_HOLD _IOW('x', 14, __u32)

/*
 * The mount's counters per NUMA node, on any fd of the mount, without
 * privilege: fills min(nr, number of node ids) records at stats_ptr and
 * sets nr to the number of node ids. EINVAL for flags other than 0,
 * EFAULT when the struct or a record cannot be copied: the records before
 * the one that failed are written, and nr is not updated.
 */
struct mxlfs_node_stat {
	__u64 pages;		/* the mount's pages on this node now */
	__u64 fallback_folios;	/* folios that preferred this node and got another */
	__u64 fallback_pages;
	__u64 moved_in_pages;	/* by MXLFS_IOC_MOVE, since the mount */
	__u64 moved_out_pages;
};

struct mxlfs_mount_nodes {
	__u64 stats_ptr;	/* in: struct mxlfs_node_stat[nr] */
	__u32 nr;		/* in/out */
	__u32 flags;		/* 0 */
};

#define MXLFS_IOC_MOUNT_NODES _IOWR('x', 15, struct mxlfs_mount_nodes)

#endif
