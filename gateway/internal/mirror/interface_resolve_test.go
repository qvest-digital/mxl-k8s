package mirror

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/go-mxl/fabrics"
)

// A setup handed a config with no address leaves interface resolution
// to fi_getinfo, which answers ENODATA on a fabric that needs a
// concrete source address. Enumerating first is what supplies one,
// along with the capabilities and message size the provider reports.

type fakeLister struct {
	got  *fabrics.InterfaceConfig
	out  []fabrics.InterfaceConfig
	err  error
	call int
}

func (f *fakeLister) Interfaces(q *fabrics.InterfaceConfig) ([]fabrics.InterfaceConfig, error) {
	f.call++
	f.got = q
	return f.out, f.err
}

func TestResolveInterfaceCarriesTheProviderAddressAndCaps(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{{
		Provider: fabrics.ProviderVerbs,
		Caps: fabrics.InterfaceCaps{
			Flags:          fabrics.InterfaceCapRemoteWrite,
			MaxMessageSize: 1 << 20,
		},
		Address: fabrics.EndpointAddress{Node: "10.20.53.13"},
	}}}

	got, err := resolveInterface(l, fabrics.ProviderVerbs, "")
	require.NoError(t, err)
	assert.Equal(t, "10.20.53.13", got.Address.Node,
		"the address the setup binds to has to come from the provider")
	assert.Equal(t, uint64(1<<20), got.Caps.MaxMessageSize,
		"maxMessageSize is only knowable from enumeration")
	assert.Equal(t, fabrics.InterfaceCapRemoteWrite, got.Caps.Flags)
	assert.Equal(t, fabrics.ProviderVerbs, got.Provider)
}

func TestResolveInterfaceLeavesServiceUnset(t *testing.T) {
	// The port belongs to the endpoint being opened, not to the
	// interface; an empty one means ephemeral.
	l := &fakeLister{out: []fabrics.InterfaceConfig{{
		Provider: fabrics.ProviderTCP,
		Caps:     fabrics.InterfaceCaps{Flags: fabrics.InterfaceCapRemoteWrite},
		Address:  fabrics.EndpointAddress{Node: "10.0.0.1", Service: "47001"},
	}}}

	got, err := resolveInterface(l, fabrics.ProviderTCP, "")
	require.NoError(t, err)
	assert.Empty(t, got.Address.Service)
}

func TestResolveInterfaceQueriesByProviderOnly(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{{
		Provider: fabrics.ProviderVerbs,
		Caps:     fabrics.InterfaceCaps{Flags: fabrics.InterfaceCapRemoteWrite},
	}}}

	_, err := resolveInterface(l, fabrics.ProviderVerbs, "")
	require.NoError(t, err)
	require.NotNil(t, l.got)
	assert.Equal(t, fabrics.ProviderVerbs, l.got.Provider)
	assert.Empty(t, l.got.Address.Node,
		"an unset bind address must not narrow the search")
}

func TestResolveInterfaceNarrowsToTheBindAddress(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{{
		Provider: fabrics.ProviderVerbs,
		Caps:     fabrics.InterfaceCaps{Flags: fabrics.InterfaceCapRemoteWrite},
		Address:  fabrics.EndpointAddress{Node: "10.20.53.13"},
	}}}

	_, err := resolveInterface(l, fabrics.ProviderVerbs, "10.20.53.13")
	require.NoError(t, err)
	require.NotNil(t, l.got)
	assert.Equal(t, "10.20.53.13", l.got.Address.Node)
}

func TestResolveInterfacePrefersTheFasterProvider(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		{Provider: fabrics.ProviderTCP, Caps: fabrics.InterfaceCaps{Flags: fabrics.InterfaceCapRemoteWrite}},
		{Provider: fabrics.ProviderVerbs, Caps: fabrics.InterfaceCaps{Flags: fabrics.InterfaceCapRemoteWrite}},
	}}

	got, err := resolveInterface(l, fabrics.ProviderAny, "")
	require.NoError(t, err)
	assert.Equal(t, fabrics.ProviderVerbs, got.Provider)
}

func TestResolveInterfaceRejectsWhenNothingCanTransfer(t *testing.T) {
	// An interface that cannot do remote write is refused by the setup
	// path anyway; failing here names the reason.
	l := &fakeLister{out: []fabrics.InterfaceConfig{{
		Provider: fabrics.ProviderVerbs,
		Caps:     fabrics.InterfaceCaps{Flags: fabrics.InterfaceCapSendReceive},
	}}}

	_, err := resolveInterface(l, fabrics.ProviderVerbs, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, errNoInterface)
}

func TestResolveInterfaceEmptyEnumeration(t *testing.T) {
	l := &fakeLister{}
	_, err := resolveInterface(l, fabrics.ProviderVerbs, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, errNoInterface)
}

func TestResolveInterfacePropagatesQueryFailure(t *testing.T) {
	sentinel := errors.New("boom")
	l := &fakeLister{err: sentinel}
	_, err := resolveInterface(l, fabrics.ProviderVerbs, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
}
