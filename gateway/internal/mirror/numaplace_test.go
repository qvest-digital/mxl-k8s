package mirror

import (
	"errors"
	"testing"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/stretchr/testify/require"
)

// stepInitiator records what setUpInitiator does to it.
type stepInitiator struct {
	steps            *[]string
	setupErr, addErr error
}

func (i *stepInitiator) Setup(fabrics.InitiatorConfig) error {
	*i.steps = append(*i.steps, "setup")
	return i.setupErr
}

func (i *stepInitiator) AddTarget(*fabrics.TargetInfo) error {
	*i.steps = append(*i.steps, "addTarget")
	return i.addErr
}

func (i *stepInitiator) Close() error {
	*i.steps = append(*i.steps, "close")
	return nil
}

// A registered ring cannot move: the placement comes before Setup, and
// on every failure it is released only after the initiator is closed,
// so no ring moves while an initiator may still hold it registered.
func TestSetUpInitiatorPlacesBeforeSetupAndReleasesAfterClose(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name      string
		setupErr  error
		parseErr  error
		addErr    error
		wantSteps []string
		wantErr   error
	}{
		{name: "attached", wantSteps: []string{"place", "setup", "parse", "addTarget"}},
		{name: "setup fails", setupErr: boom,
			wantSteps: []string{"place", "setup", "close", "release"}},
		{name: "target info unparseable", parseErr: boom,
			wantSteps: []string{"place", "setup", "parse", "close", "release"}},
		{name: "add target fails", addErr: boom, wantErr: errAddTargetFailed,
			wantSteps: []string{"place", "setup", "parse", "addTarget", "close", "release"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var steps []string
			in := &stepInitiator{steps: &steps, setupErr: tc.setupErr, addErr: tc.addErr}
			place := func() func() {
				steps = append(steps, "place")
				return func() { steps = append(steps, "release") }
			}
			parse := func(string) (*fabrics.TargetInfo, error) {
				steps = append(steps, "parse")
				if tc.parseErr != nil {
					return nil, tc.parseErr
				}
				return &fabrics.TargetInfo{}, nil
			}
			release, info, err := setUpInitiator(in, place, fabrics.InitiatorConfig{}, "info", parse)
			require.Equal(t, tc.wantSteps, steps)
			if tc.wantSteps[len(tc.wantSteps)-1] == "addTarget" {
				require.NoError(t, err)
				require.NotNil(t, info)
				release()
				require.Equal(t, "release", steps[len(steps)-1], "the link's release is the placement's")
				return
			}
			require.Error(t, err)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			}
			require.Nil(t, release)
		})
	}
}

// detach releases the link's placement once its initiator is closed.
func TestDetachReleasesPlacement(t *testing.T) {
	released := 0
	link := &sourceLink{initiator: &fabrics.Initiator{}, release: func() { released++ }}
	(&libmxlOpener{}).detach(link)
	require.Equal(t, 1, released)
}

func TestAllocateRingHoldsFirstAndDropsOnFailure(t *testing.T) {
	var steps []string
	hold := func() func() {
		steps = append(steps, "hold")
		return func() { steps = append(steps, "release") }
	}
	release, err := allocateRing(hold, func() error { steps = append(steps, "allocate"); return nil })
	require.NoError(t, err)
	require.Equal(t, []string{"hold", "allocate"}, steps)
	release()
	require.Equal(t, []string{"hold", "allocate", "release"}, steps)

	steps = nil
	release, err = allocateRing(hold, func() error { steps = append(steps, "allocate"); return errors.New("enospc") })
	require.Error(t, err)
	require.Nil(t, release)
	require.Equal(t, []string{"hold", "allocate", "release"}, steps)
}

// The holds on a target's ring go with its writer, on both ways an entry
// gives the writer up.
func TestTargetRingHoldsGoWithTheWriter(t *testing.T) {
	released := 0
	e := &targetEntry{ringRelease: func() { released++ }}
	require.True(t, closeTargetHandles(e, keepFlow, 0))
	require.Equal(t, 1, released)
	require.True(t, closeTargetHandles(e, keepFlow, 0))
	require.Equal(t, 1, released)

	e = &targetEntry{ringRelease: func() { released++ }}
	require.Nil(t, e.abandon())
	require.Equal(t, 2, released)
	require.Nil(t, e.abandon())
	require.Equal(t, 2, released)
}
