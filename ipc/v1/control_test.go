package ipcv1

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// The wire format of these messages is the only thing pinning the
// agent and gateway together across releases. The tests below freeze
// each enum number and each message round-trip; a future regen that
// silently changes field numbers or enum positions will fail before it
// can produce CRs neither component can decode.

func TestProviderEnum_ZeroAndValues(t *testing.T) {
	t.Run("zero value is unspecified", func(t *testing.T) {
		var zero Provider
		assert.Equal(t, Provider_PROVIDER_UNSPECIFIED, zero)
	})

	t.Run("numeric values are stable", func(t *testing.T) {
		// Changing any of these means an in-flight gRPC message from
		// an older peer is silently reinterpreted. Pin them.
		assert.Equal(t, int32(0), int32(Provider_PROVIDER_UNSPECIFIED))
		assert.Equal(t, int32(1), int32(Provider_PROVIDER_AUTO))
		assert.Equal(t, int32(2), int32(Provider_PROVIDER_TCP))
		assert.Equal(t, int32(3), int32(Provider_PROVIDER_VERBS))
		assert.Equal(t, int32(4), int32(Provider_PROVIDER_EFA))
		assert.Equal(t, int32(5), int32(Provider_PROVIDER_SHM))
	})

	t.Run("name table is exhaustive", func(t *testing.T) {
		// Every numeric value in the const block has a name entry; a
		// missing entry would make .String() return the number, not
		// the symbolic form, which is what the logs and the
		// gateway-side translator depend on.
		want := map[int32]string{
			0: "PROVIDER_UNSPECIFIED",
			1: "PROVIDER_AUTO",
			2: "PROVIDER_TCP",
			3: "PROVIDER_VERBS",
			4: "PROVIDER_EFA",
			5: "PROVIDER_SHM",
		}
		if diff := cmp.Diff(want, Provider_name); diff != "" {
			t.Fatalf("Provider_name diverged (-want +got):\n%s", diff)
		}
	})
}

func TestDirectionEnum_ZeroAndValues(t *testing.T) {
	t.Run("zero value is unspecified", func(t *testing.T) {
		var zero Direction
		assert.Equal(t, Direction_DIRECTION_UNSPECIFIED, zero)
	})

	t.Run("numeric values are stable", func(t *testing.T) {
		assert.Equal(t, int32(0), int32(Direction_DIRECTION_UNSPECIFIED))
		assert.Equal(t, int32(1), int32(Direction_DIRECTION_INGRESS))
		assert.Equal(t, int32(2), int32(Direction_DIRECTION_EGRESS))
	})

	t.Run("name table is exhaustive", func(t *testing.T) {
		want := map[int32]string{
			0: "DIRECTION_UNSPECIFIED",
			1: "DIRECTION_INGRESS",
			2: "DIRECTION_EGRESS",
		}
		if diff := cmp.Diff(want, Direction_name); diff != "" {
			t.Fatalf("Direction_name diverged (-want +got):\n%s", diff)
		}
	})
}

func TestMessages_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		msg  proto.Message
	}{
		{
			"OpenMirrorRequest",
			&OpenMirrorRequest{
				FlowId:     "11111111-2222-3333-4444-555555555555",
				SourceNode: "node-a",
				Provider:   Provider_PROVIDER_TCP,
			},
		},
		{
			"OpenMirrorResponse",
			&OpenMirrorResponse{TargetInfo: "info-string"},
		},
		{
			"CloseMirrorRequest",
			&CloseMirrorRequest{FlowId: "11111111-2222-3333-4444-555555555555"},
		},
		{
			"CloseMirrorResponse",
			&CloseMirrorResponse{},
		},
		{
			"ListLocalEndpointsRequest",
			&ListLocalEndpointsRequest{},
		},
		{
			"ListLocalEndpointsResponse",
			&ListLocalEndpointsResponse{
				Endpoints: []*LocalEndpoint{
					{
						FlowId:     "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
						Provider:   Provider_PROVIDER_VERBS,
						Direction:  Direction_DIRECTION_INGRESS,
						TargetInfo: "info",
					},
					{
						FlowId:    "11111111-2222-3333-4444-555555555555",
						Provider:  Provider_PROVIDER_TCP,
						Direction: Direction_DIRECTION_EGRESS,
					},
				},
			},
		},
	}

	opts := []cmp.Option{
		cmpopts.IgnoreUnexported(
			OpenMirrorRequest{}, OpenMirrorResponse{},
			CloseMirrorRequest{}, CloseMirrorResponse{},
			ListLocalEndpointsRequest{}, ListLocalEndpointsResponse{},
			LocalEndpoint{},
		),
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := proto.Marshal(tc.msg)
			require.NoError(t, err)

			// A fresh empty instance of the same concrete type.
			out := proto.Clone(tc.msg)
			proto.Reset(out)
			require.NoError(t, proto.Unmarshal(wire, out))

			if diff := cmp.Diff(tc.msg, out, opts...); diff != "" {
				t.Fatalf("round-trip diverged (-orig +decoded):\n%s", diff)
			}
		})
	}
}

func TestLocalEndpoint_IngressEgressDifferAfterRoundTrip(t *testing.T) {
	// LocalEndpoint.TargetInfo is documented as empty for egress
	// endpoints. proto3 strings serialise the empty value as "no field
	// present", which round-trips back to "". This is the property the
	// agent relies on when discriminating ingress vs egress in the
	// gateway response; the test pins it so a future migration to
	// proto2 (or an `optional` keyword on the field) would surface.
	ingress := &LocalEndpoint{
		FlowId:     "11111111-2222-3333-4444-555555555555",
		Direction:  Direction_DIRECTION_INGRESS,
		TargetInfo: "info",
	}
	egress := &LocalEndpoint{
		FlowId:    "11111111-2222-3333-4444-555555555555",
		Direction: Direction_DIRECTION_EGRESS,
	}

	for _, in := range []*LocalEndpoint{ingress, egress} {
		wire, err := proto.Marshal(in)
		require.NoError(t, err)
		out := &LocalEndpoint{}
		require.NoError(t, proto.Unmarshal(wire, out))
		assert.Equal(t, in.TargetInfo, out.TargetInfo)
	}
}

func TestUnknownEnumValue_DoesNotPanic(t *testing.T) {
	// proto3 must preserve unknown enum numbers across versions: an
	// older peer that knows providers {auto..shm} must not crash when
	// it receives a wire message produced by a newer peer that added
	// PROVIDER_NEW = 6. The test simulates that by serialising the
	// known message with a numeric value the const block does not
	// declare, then deserialising back.
	src := &OpenMirrorRequest{
		FlowId:   "11111111-2222-3333-4444-555555555555",
		Provider: Provider(99),
	}
	wire, err := proto.Marshal(src)
	require.NoError(t, err)

	out := &OpenMirrorRequest{}
	require.NoError(t, proto.Unmarshal(wire, out))
	assert.Equal(t, Provider(99), out.Provider,
		"proto3 must round-trip unknown enum numbers verbatim; if "+
			"this fails, peer-version skew turns into silent data loss")
}
