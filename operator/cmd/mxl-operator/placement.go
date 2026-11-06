package main

import (
	"flag"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/qvest-digital/mxl-k8s/operator/internal/placement"
)

// placementFlags are the operator's mirror placement settings.
type placementFlags struct {
	maxUtilization  int
	overheadPercent int
	provisionalRaw  string
	provisional     int64
	demandWait      time.Duration
	pairing         string
	overcommit      string
}

func (p *placementFlags) bind(fs *flag.FlagSet) {
	fs.IntVar(&p.maxUtilization, "placement-max-utilization", placement.DefaultMaxUtilizationPercent,
		"Percent of a fabric interface's or shared budget's capacity mirror placement may commit, "+
			"on nodes whose gateway sets no value of its own. Grains leave as line-rate bursts, so "+
			"average load at capacity already overflows a receiver.")
	fs.IntVar(&p.overheadPercent, "placement-protocol-overhead", placement.DefaultOverheadPercent,
		"Percent added to a flow's published payload rate for protocol framing when charging it to an interface.")
	fs.StringVar(&p.provisionalRaw, "placement-provisional-demand", "0",
		"Rate in bits per second charged for a flow that has not published its payload rate within "+
			"--placement-demand-wait, as a quantity (for example 3G). Zero places such mirrors by "+
			"mirror count alone.")
	fs.DurationVar(&p.demandWait, "placement-demand-wait", placement.DefaultDemandWait,
		"How long after its creation a mirror waits for its flow's payload rate before it is placed "+
			"on the provisional demand.")
	fs.StringVar(&p.pairing, "placement-pairing", string(placement.PairingPreferSameSubnet),
		"How a source interface pairs with a target interface: prefer-same-subnet, same-subnet or any. "+
			"With every fabric port in one subnet all pairs qualify; it matters where ports sit in separate subnets.")
	fs.StringVar(&p.overcommit, "placement-overcommit", string(placement.OvercommitPlace),
		"What happens to a mirror no interface pair has room for: place puts it on the pair with the "+
			"most headroom and reports Overcommitted, hold leaves it unplaced and reports Held.")
}

func (p *placementFlags) validate() error {
	if p.maxUtilization < 1 || p.maxUtilization > 100 {
		return fmt.Errorf("--placement-max-utilization must be 1..100, got %d", p.maxUtilization)
	}
	if p.overheadPercent < 0 || p.overheadPercent > 100 {
		return fmt.Errorf("--placement-protocol-overhead must be 0..100, got %d", p.overheadPercent)
	}
	q, err := resource.ParseQuantity(p.provisionalRaw)
	if err != nil {
		return fmt.Errorf("--placement-provisional-demand: %w", err)
	}
	v, ok := q.AsInt64()
	if !ok || v < 0 {
		return fmt.Errorf("--placement-provisional-demand must be a non-negative whole number of bits per second, got %q", p.provisionalRaw)
	}
	p.provisional = v
	if p.demandWait < 0 {
		return fmt.Errorf("--placement-demand-wait must not be negative, got %v", p.demandWait)
	}
	switch placement.Pairing(p.pairing) {
	case placement.PairingPreferSameSubnet, placement.PairingSameSubnet, placement.PairingAny:
	default:
		return fmt.Errorf("--placement-pairing must be prefer-same-subnet, same-subnet or any, got %q", p.pairing)
	}
	switch placement.Overcommit(p.overcommit) {
	case placement.OvercommitPlace, placement.OvercommitHold:
	default:
		return fmt.Errorf("--placement-overcommit must be place or hold, got %q", p.overcommit)
	}
	return nil
}

func (p *placementFlags) settings() placement.Settings {
	return placement.Settings{
		MaxUtilizationPercent: int32(p.maxUtilization),
		Pairing:               placement.Pairing(p.pairing),
		Overcommit:            placement.Overcommit(p.overcommit),
	}
}
