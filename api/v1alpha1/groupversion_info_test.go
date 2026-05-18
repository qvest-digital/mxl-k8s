package v1alpha1

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// expectedKinds enumerates every CRD kind this package serves. The map
// pairs the kind name (the value of the kubebuilder:resource marker's
// effective Kind, i.e. the Go struct name) with a zero-valued instance
// the test can register and look up. A new CRD added to the package
// without an entry here fails the test, forcing the new type to be
// added to addKnownTypes alongside this map.
var expectedKinds = map[string]struct {
	item runtime.Object
	list runtime.Object
}{
	"MxlDomain":           {&MxlDomain{}, &MxlDomainList{}},
	"MxlFlow":             {&MxlFlow{}, &MxlFlowList{}},
	"MxlFlowMirror":       {&MxlFlowMirror{}, &MxlFlowMirrorList{}},
	"MxlReceiver":         {&MxlReceiver{}, &MxlReceiverList{}},
	"MxlNodeCapabilities": {&MxlNodeCapabilities{}, &MxlNodeCapabilitiesList{}},
}

func TestGroupVersion(t *testing.T) {
	assert.Equal(t, "mxl.qvest-digital.com", GroupVersion.Group)
	assert.Equal(t, "v1alpha1", GroupVersion.Version)
}

func TestAddToScheme_RegistersEveryKind(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, AddToScheme(scheme))

	for kind, pair := range expectedKinds {
		t.Run(kind, func(t *testing.T) {
			gvk := schema.GroupVersionKind{
				Group:   GroupVersion.Group,
				Version: GroupVersion.Version,
				Kind:    kind,
			}
			gvkList := schema.GroupVersionKind{
				Group:   GroupVersion.Group,
				Version: GroupVersion.Version,
				Kind:    kind + "List",
			}

			assert.Truef(t, scheme.Recognizes(gvk),
				"scheme did not register the %s kind; addKnownTypes "+
					"likely lost the entry", kind)
			assert.Truef(t, scheme.Recognizes(gvkList),
				"scheme did not register the %sList kind", kind)

			gvks, _, err := scheme.ObjectKinds(pair.item)
			require.NoError(t, err)
			require.Contains(t, gvks, gvk,
				"the registered Go type %T does not resolve back to %v",
				pair.item, gvk)

			gvks, _, err = scheme.ObjectKinds(pair.list)
			require.NoError(t, err)
			require.Contains(t, gvks, gvkList,
				"the registered Go list type %T does not resolve back to %v",
				pair.list, gvkList)

			// Round-trip a zero value through scheme.New to ensure the
			// returned object is a non-nil pointer of the same dynamic
			// type the test registered. The cache-warming codepath in
			// client-go calls scheme.New for every watch event; a
			// silent regression here would surface as nil-pointer
			// panics under load instead of at compile time.
			fresh, err := scheme.New(gvk)
			require.NoError(t, err)
			require.NotNil(t, fresh)
			assert.Equal(t,
				reflect.TypeOf(pair.item),
				reflect.TypeOf(fresh),
				"scheme.New returned %T, expected %T", fresh, pair.item)
		})
	}
}

func TestAddToScheme_NoExtraKindsLeak(t *testing.T) {
	// The inverse of the previous test: every kind the scheme knows
	// about under GroupVersion must appear in expectedKinds (modulo
	// List companions and the metav1.AddToGroupVersion machinery).
	// Catches a stray AddKnownTypes call that registered a Go type
	// nobody added to the manifest above.
	scheme := runtime.NewScheme()
	require.NoError(t, AddToScheme(scheme))

	knownByGV := scheme.KnownTypes(GroupVersion)

	allowed := map[string]struct{}{
		// metav1.AddToGroupVersion adds these. They are not part of
		// this package's CRD surface but must remain present for list
		// and watch operations to work.
		"WatchEvent":                {},
		"APIGroup":                  {},
		"APIGroupList":              {},
		"APIResourceList":           {},
		"APIVersions":               {},
		"CreateOptions":             {},
		"DeleteOptions":             {},
		"GetOptions":                {},
		"ListOptions":               {},
		"PatchOptions":              {},
		"UpdateOptions":             {},
		"Status":                    {},
		"Table":                     {},
		"TableOptions":              {},
		"PartialObjectMetadata":     {},
		"PartialObjectMetadataList": {},
	}
	for k := range expectedKinds {
		allowed[k] = struct{}{}
		allowed[k+"List"] = struct{}{}
	}

	for kind := range knownByGV {
		_, ok := allowed[kind]
		assert.Truef(t, ok,
			"kind %q is registered with the scheme but is not "+
				"listed in expectedKinds; either add it to the test "+
				"manifest or remove the AddKnownTypes call that "+
				"registered it",
			kind)
	}
}
