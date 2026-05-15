// Package fanotify is a thin wrapper around the Linux fanotify(7)
// subsystem in FAN_REPORT_DFID_NAME mode. It surfaces directory-
// modification events (CREATE / MOVED_TO / DELETE / MOVED_FROM) on
// a single inode-marked directory.
//
// Requires kernel ≥ 5.17 and CAP_SYS_ADMIN. Building on non-Linux
// platforms yields an empty package.
package fanotify
