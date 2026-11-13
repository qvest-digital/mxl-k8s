package portmetrics

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mdlayher/genetlink"
	"github.com/mdlayher/netlink"
	"github.com/mdlayher/netlink/nlenc"
	"golang.org/x/sys/unix"
)

// MACStats are the IEEE 802.3 MAC counters the kernel's ethtool
// standard statistics report for a netdev (group eth-mac). Each is
// set only when the driver reports it.
type MACStats struct {
	OctetsReceived, OctetsTransmitted, FCSErrors          uint64
	HaveOctetsReceived, HaveOctetsTransmitted, HaveFCSErr bool
}

// PauseStats are the 802.3x pause frames a netdev sent and received.
type PauseStats struct {
	Tx, Rx         uint64
	HaveTx, HaveRx bool
}

// Ethtool reads a netdev's standard statistics.
type Ethtool interface {
	MACStats(netdev string) (MACStats, error)
	PauseStats(netdev string) (PauseStats, error)
}

// Constants of the ethtool netlink interface not in x/sys/unix
// (include/uapi/linux/ethtool_netlink_generated.h and
// ethtool_netlink.h).
const (
	ethtoolAStatsHeader = 2
	ethtoolAStatsGroups = 3
	ethtoolAStatsGrp    = 4

	ethtoolAStatsGrpID   = 2
	ethtoolAStatsGrpStat = 4

	ethtoolStatsEthMAC = 1

	ethMACFCSErr   = 4
	ethMACTxOctets = 6
	ethMACRxOctets = 12
)

// Conn is the part of a generic netlink connection Netlink uses;
// *genetlink.Conn implements it.
type Conn interface {
	GetFamily(name string) (genetlink.Family, error)
	Execute(m genetlink.Message, family uint16, flags netlink.HeaderFlags) ([]genetlink.Message, error)
	SetDeadline(t time.Time) error
	Close() error
}

// Defaults for Netlink's bounds.
const (
	DefaultNetlinkTimeout = 2 * time.Second
	DefaultNetlinkBackoff = time.Minute
)

// Netlink reads standard statistics over the ethtool generic netlink
// family, on one connection opened on first use.
//
// The kernel answers ethtool requests under the RTNL lock, so a driver
// holding that lock delays the reply. Every request has a deadline, and
// a request that times out stops the next ones for Backoff: scrapes are
// serialized on the connection, and each would otherwise wait out its
// own deadline. Any failed request drops the connection, so a reply
// arriving late is never read as the answer to a later request.
type Netlink struct {
	// Dial opens the connection; nil dials the kernel.
	Dial func() (Conn, error)
	// Timeout bounds one request; zero is DefaultNetlinkTimeout.
	Timeout time.Duration
	// Backoff is how long requests fail fast after one timed out; zero
	// is DefaultNetlinkBackoff.
	Backoff time.Duration

	mu        sync.Mutex
	conn      Conn
	family    genetlink.Family
	pausedTil time.Time
}

func (n *Netlink) open() error {
	if n.conn != nil {
		return nil
	}
	dial := n.Dial
	if dial == nil {
		dial = func() (Conn, error) { return genetlink.Dial(nil) }
	}
	c, err := dial()
	if err != nil {
		return fmt.Errorf("dial generic netlink: %w", err)
	}
	if err := c.SetDeadline(time.Now().Add(n.timeout())); err != nil {
		_ = c.Close()
		return fmt.Errorf("set netlink deadline: %w", err)
	}
	f, err := c.GetFamily(unix.ETHTOOL_GENL_NAME)
	if err != nil {
		_ = c.Close()
		return fmt.Errorf("resolve ethtool netlink family: %w", err)
	}
	n.conn, n.family = c, f
	return nil
}

func (n *Netlink) timeout() time.Duration {
	if n.Timeout > 0 {
		return n.Timeout
	}
	return DefaultNetlinkTimeout
}

// drop closes the connection after a failed request.
func (n *Netlink) drop() {
	if n.conn != nil {
		_ = n.conn.Close()
		n.conn = nil
	}
}

// Close closes the connection.
func (n *Netlink) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conn == nil {
		return nil
	}
	err := n.conn.Close()
	n.conn = nil
	return err
}

func (n *Netlink) get(cmd uint8, encode func(*netlink.AttributeEncoder)) ([]genetlink.Message, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if now := time.Now(); now.Before(n.pausedTil) {
		return nil, fmt.Errorf("ethtool netlink paused until %s after a request timed out", n.pausedTil.Format(time.RFC3339))
	}
	ae := netlink.NewAttributeEncoder()
	encode(ae)
	body, err := ae.Encode()
	if err != nil {
		return nil, err
	}
	msgs, err := n.execute(cmd, body)
	if err != nil {
		n.drop()
		if errors.Is(err, os.ErrDeadlineExceeded) {
			backoff := n.Backoff
			if backoff <= 0 {
				backoff = DefaultNetlinkBackoff
			}
			n.pausedTil = time.Now().Add(backoff)
		}
		return nil, err
	}
	return msgs, nil
}

func (n *Netlink) execute(cmd uint8, body []byte) ([]genetlink.Message, error) {
	if err := n.open(); err != nil {
		return nil, err
	}
	if err := n.conn.SetDeadline(time.Now().Add(n.timeout())); err != nil {
		return nil, fmt.Errorf("set netlink deadline: %w", err)
	}
	return n.conn.Execute(genetlink.Message{
		Header: genetlink.Header{Command: cmd, Version: n.family.Version},
		Data:   body,
	}, n.family.ID, netlink.Request)
}

func header(netdev string, flags uint32) func(*netlink.AttributeEncoder) {
	return func(ae *netlink.AttributeEncoder) {
		ae.String(unix.ETHTOOL_A_HEADER_DEV_NAME, netdev)
		if flags != 0 {
			ae.Uint32(unix.ETHTOOL_A_HEADER_FLAGS, flags)
		}
	}
}

// MACStats implements Ethtool.
func (n *Netlink) MACStats(netdev string) (MACStats, error) {
	msgs, err := n.get(unix.ETHTOOL_MSG_STATS_GET, func(ae *netlink.AttributeEncoder) {
		ae.Nested(ethtoolAStatsHeader, func(nae *netlink.AttributeEncoder) error {
			header(netdev, 0)(nae)
			return nil
		})
		// A compact bitset holding only the eth-mac group.
		ae.Nested(ethtoolAStatsGroups, func(nae *netlink.AttributeEncoder) error {
			nae.Flag(unix.ETHTOOL_A_BITSET_NOMASK, true)
			nae.Uint32(unix.ETHTOOL_A_BITSET_SIZE, ethtoolStatsEthMAC+1)
			nae.Bytes(unix.ETHTOOL_A_BITSET_VALUE, nlenc32(1<<ethtoolStatsEthMAC))
			return nil
		})
	})
	if err != nil {
		return MACStats{}, err
	}
	var out MACStats
	for _, m := range msgs {
		ad, err := netlink.NewAttributeDecoder(m.Data)
		if err != nil {
			return MACStats{}, err
		}
		for ad.Next() {
			if ad.Type() != ethtoolAStatsGrp {
				continue
			}
			ad.Nested(func(gd *netlink.AttributeDecoder) error {
				var id uint32
				stats := map[uint16]uint64{}
				for gd.Next() {
					switch gd.Type() {
					case ethtoolAStatsGrpID:
						id = gd.Uint32()
					case ethtoolAStatsGrpStat:
						// One u64 attribute whose type is the statistic.
						gd.Nested(func(sd *netlink.AttributeDecoder) error {
							for sd.Next() {
								stats[sd.Type()] = sd.Uint64()
							}
							return nil
						})
					}
				}
				if id != ethtoolStatsEthMAC {
					return nil
				}
				out.OctetsReceived, out.HaveOctetsReceived = stats[ethMACRxOctets], has(stats, ethMACRxOctets)
				out.OctetsTransmitted, out.HaveOctetsTransmitted = stats[ethMACTxOctets], has(stats, ethMACTxOctets)
				out.FCSErrors, out.HaveFCSErr = stats[ethMACFCSErr], has(stats, ethMACFCSErr)
				return nil
			})
		}
		if err := ad.Err(); err != nil {
			return MACStats{}, err
		}
	}
	return out, nil
}

// PauseStats implements Ethtool.
func (n *Netlink) PauseStats(netdev string) (PauseStats, error) {
	msgs, err := n.get(unix.ETHTOOL_MSG_PAUSE_GET, func(ae *netlink.AttributeEncoder) {
		ae.Nested(unix.ETHTOOL_A_PAUSE_HEADER, func(nae *netlink.AttributeEncoder) error {
			header(netdev, unix.ETHTOOL_FLAG_STATS)(nae)
			return nil
		})
	})
	if err != nil {
		return PauseStats{}, err
	}
	var out PauseStats
	for _, m := range msgs {
		ad, err := netlink.NewAttributeDecoder(m.Data)
		if err != nil {
			return PauseStats{}, err
		}
		for ad.Next() {
			if ad.Type() != unix.ETHTOOL_A_PAUSE_STATS {
				continue
			}
			ad.Nested(func(sd *netlink.AttributeDecoder) error {
				for sd.Next() {
					switch sd.Type() {
					case unix.ETHTOOL_A_PAUSE_STAT_TX_FRAMES:
						out.Tx, out.HaveTx = sd.Uint64(), true
					case unix.ETHTOOL_A_PAUSE_STAT_RX_FRAMES:
						out.Rx, out.HaveRx = sd.Uint64(), true
					}
				}
				return nil
			})
		}
		if err := ad.Err(); err != nil {
			return PauseStats{}, err
		}
	}
	return out, nil
}

func has(m map[uint16]uint64, k uint16) bool {
	_, ok := m[k]
	return ok
}

// nlenc32 is v in host byte order, as a bitset word.
func nlenc32(v uint32) []byte {
	b := make([]byte, 4)
	nlenc.PutUint32(b, v)
	return b
}
