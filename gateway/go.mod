module github.com/qvest-digital/mxl-k8s/gateway

go 1.26.0

require (
	k8s.io/apimachinery v0.36.1
	k8s.io/client-go v0.36.1
	sigs.k8s.io/controller-runtime v0.24.1
)

// github.com/qvest-digital/go-mxl is intentionally not listed in
// require: it has no tagged release yet, and CI keeps the gateway
// in the gated-and-warned lane until one ships. Add
// `require github.com/qvest-digital/go-mxl vX.Y.Z` once go-mxl is
// tagged and `go mod tidy` will populate go.sum.
//
// github.com/qvest-digital/mxl-k8s/api is in-repo and resolved via
// the replace below.

replace github.com/qvest-digital/mxl-k8s/api => ../api
