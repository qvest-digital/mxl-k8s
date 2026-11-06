package portmetrics

import (
	"fmt"
	"sync"

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

// Netlink reads standard statistics over the ethtool generic netlink
// family. It opens one connection, on first use.
type Netlink struct {
	// Dial opens the connection; nil dials the kernel.
	Dial func() (*genetlink.Conn, error)

	mu     sync.Mutex
	conn   *genetlink.Conn
	family genetlink.Family
}

func (n *Netlink) open() error {
	if n.conn != nil {
		return nil
	}
	dial := n.Dial
	if dial == nil {
		dial = func() (*genetlink.Conn, error) { return genetlink.Dial(nil) }
	}
	c, err := dial()
	if err != nil {
		return fmt.Errorf("dial generic netlink: %w", err)
	}
	f, err := c.GetFamily(unix.ETHTOOL_GENL_NAME)
	if err != nil {
		_ = c.Close()
		return fmt.Errorf("resolve ethtool netlink family: %w", err)
	}
	n.conn, n.family = c, f
	return nil
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
	if err := n.open(); err != nil {
		return nil, err
	}
	ae := netlink.NewAttributeEncoder()
	encode(ae)
	body, err := ae.Encode()
	if err != nil {
		return nil, err
	}
	msgs, err := n.conn.Execute(genetlink.Message{
		Header: genetlink.Header{Command: cmd, Version: n.family.Version},
		Data:   body,
	}, n.family.ID, netlink.Request)
	if err != nil {
		return nil, err
	}
	return msgs, nil
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
