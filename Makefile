CONTROLLER_TOOLS_VERSION ?= v0.18.0
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

GOTESTSUM_VERSION ?= v1.13.0
GOTESTSUM ?= go run gotest.tools/gotestsum@$(GOTESTSUM_VERSION)
MOCKERY_VERSION ?= v3.5.4
MOCKERY ?= go run github.com/vektra/mockery/v3@$(MOCKERY_VERSION)
SETUP_ENVTEST_VERSION ?= release-0.22
SETUP_ENVTEST ?= go run sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION)
ENVTEST_K8S_VERSION ?= 1.31.0
ENVTEST_DIR ?= $(CURDIR)/bin
COVERAGE_DIR ?= $(CURDIR)/bin
TEST_TIMEOUT ?= 5m
TEST_ARGS ?= -race -timeout $(TEST_TIMEOUT)

MODULES := api operator agent gateway
PURE_TEST_MODULES := api operator agent

.PHONY: all
all: gen-api lint build

.PHONY: lint
lint:
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then \
		echo "Files need gofmt:"; echo "$$out"; exit 1; \
	fi
	@for m in $(MODULES); do \
		echo ">> go vet ./... in $$m"; \
		(cd $$m && go vet ./...) || exit $$?; \
	done

.PHONY: build
build:
	@for m in $(MODULES); do \
		echo ">> go build ./... in $$m"; \
		(cd $$m && go build ./...) || exit $$?; \
	done

.PHONY: gen-api
gen-api:
	cd api && $(CONTROLLER_GEN) object paths=./...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:dir=./config/crd

.PHONY: gen-rbac
gen-rbac:
	$(CONTROLLER_GEN) rbac:roleName=mxl-operator paths=./operator/... output:rbac:dir=./config/rbac

CRD_GEN_PATHS := config/ api/v1alpha1/zz_generated.deepcopy.go

.PHONY: manifests-check
manifests-check: gen-api gen-rbac
	@if ! git diff --exit-code -- $(CRD_GEN_PATHS); then \
		echo "Generated CRD/DeepCopy/RBAC files are out of sync."; \
		echo "Run 'make gen-api gen-rbac' and commit the result."; \
		exit 1; \
	fi

.PHONY: chart-crd-sync
chart-crd-sync:
	@mkdir -p charts/mxl-k8s/crds
	@cp config/crd/mxl.qvest-digital.com_*.yaml charts/mxl-k8s/crds/

.PHONY: chart-schema
chart-schema:
	$(HELM_SCHEMA) --chart-search-root charts/mxl-k8s --append-newline \
	    --helm-docs-compatibility-mode --skip-auto-generation additionalProperties

.PHONY: chart-docs
chart-docs:
	$(HELM_DOCS) --chart-search-root charts

.PHONY: chart-lint
chart-lint:
	helm lint charts/mxl-k8s

.PHONY: chart-test
chart-test: chart-lint
	helm unittest charts/mxl-k8s -f 'tests/unit/*_test.yaml'

.PHONY: chart-check
chart-check: chart-crd-sync chart-schema chart-docs
	@if ! git diff --exit-code -- charts/mxl-k8s/crds charts/mxl-k8s/values.schema.json charts/mxl-k8s/README.md; then \
		echo "Chart generated artefacts are out of sync."; \
		echo "Run 'make chart-crd-sync chart-schema chart-docs' and commit the result."; \
		exit 1; \
	fi

# Pin per-component image tags in charts/mxl-k8s/values.yaml so a
# local `helm install ./charts/mxl-k8s` or `helm template
# ./charts/mxl-k8s` resolves to the same image tags the CI-published
# chart would carry. `MODE` is one of `dev`, `rc`, `stable` (default
# `rc`). The chart workflow runs the same script with the chart's
# version at package time; local users can match either flow.
# `make chart-resolve-reset` reverts values.yaml.
MODE ?= rc

.PHONY: chart-resolve
chart-resolve:
	@bash hack/chart-resolve-tags.sh $(MODE)

.PHONY: chart-resolve-reset
chart-resolve-reset:
	git checkout -- charts/mxl-k8s/values.yaml

HELM_SCHEMA ?= $(shell go env GOPATH)/bin/helm-schema
HELM_DOCS   ?= $(shell go env GOPATH)/bin/helm-docs

.PHONY: generated-check
generated-check: manifests-check chart-check

# --- Test targets ---
# `make test`         runs unit tests across pure-Go modules (api,
#                     operator, agent). The operator suite needs the
#                     kube-apiserver + etcd binaries that envtest provides;
#                     they are fetched into $(ENVTEST_DIR) on demand and
#                     reused thereafter.
# `make test-gateway` runs gateway tests inside the cgo lane (libmxl +
#                     libmxl-fabrics must be installed for the package to
#                     compile). CI runs this in the go-mxl-builder image.
# `make test-all`     runs both lanes.
# `make mocks`        regenerates testify-style mocks listed in
#                     `.mockery.yaml`. `make mocks-check` fails the build
#                     if generated mocks are out of sync with the
#                     committed copies.
#
# JUnit XML and coverage profiles land under bin/ keyed by module name.

.PHONY: envtest-assets
envtest-assets:
	@mkdir -p $(ENVTEST_DIR)
	@$(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir=$(ENVTEST_DIR) >/dev/null

.PHONY: envtest-path
envtest-path:
	@$(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir=$(ENVTEST_DIR) -p path

define test_module
	@echo ">> test $(1)"
	@mkdir -p $(COVERAGE_DIR)
	@cd $(1) && KUBEBUILDER_ASSETS="$$(cd $(CURDIR) && $(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir=$(ENVTEST_DIR) -p path)" \
	    $(GOTESTSUM) \
	        --format=testname \
	        --junitfile=$(COVERAGE_DIR)/junit-$(1).xml \
	        --jsonfile=$(COVERAGE_DIR)/test-$(1).json \
	        -- $(TEST_ARGS) \
	        -coverprofile=$(COVERAGE_DIR)/cover-$(1).out \
	        -covermode=atomic \
	        ./...
endef

.PHONY: test
test: envtest-assets
	@for m in $(PURE_TEST_MODULES); do \
	    $(MAKE) --no-print-directory test-one MODULE=$$m || exit $$?; \
	done

.PHONY: test-one
test-one:
	$(call test_module,$(MODULE))

.PHONY: test-gateway
test-gateway:
	$(call test_module,gateway)

.PHONY: test-all
test-all: test test-gateway

.PHONY: coverage-report
coverage-report:
	@for m in $(MODULES); do \
	    f=$(COVERAGE_DIR)/cover-$$m.out; \
	    if [ -f $$f ]; then \
	        echo "=== $$m ==="; \
	        (cd $$m && go tool cover -func=$$f | tail -n 1); \
	    fi; \
	done

.PHONY: mocks
mocks:
	$(MOCKERY)

.PHONY: mocks-check
mocks-check: mocks
	@if ! git diff --exit-code -- '**/mocks/*.go'; then \
	    echo "mocks out of date; run 'make mocks' and commit the result."; \
	    exit 1; \
	fi

# --- KIND demo helpers ---
# `make kind-up`     builds the five component images, creates (or
#                    reuses) a 3-node KIND cluster, loads the images,
#                    installs the mxl-k8s Helm chart against
#                    examples/kind/values.yaml, applies the demo
#                    workload from examples/kind/demo/, and waits
#                    for the MxlFlowMirror to reach Ready.
# `make kind-down`   deletes the cluster.
# `make kind-status` prints a quick status summary.
# `make kind-test`   runs the integration suite in
#                    test/integration/kind against the cluster spun
#                    up by `make kind-up`. Failure diagnostics land
#                    under KIND_DIAG_DIR for the kind-integration
#                    GitHub Actions job to upload as an artifact.
#
# Requires: docker (or podman), kind, kubectl, helm.
#
# Override the cluster name with KIND_CLUSTER=<name>.
# Use Podman instead of Docker: CONTAINER_RUNTIME=podman
#   e.g. `make kind-up CONTAINER_RUNTIME=podman`
#
# Image source selector (BUILD):
#   unset / BUILD=local  build the five component images locally as
#                        ghcr.io/qvest-digital/mxl-k8s/<comp>:dev and
#                        kind-load them (existing behaviour).
#   BUILD=<tag>          skip the local build; pull
#                        ghcr.io/<owner>/mxl-k8s/<component>:<tag> for
#                        every component, kind-load it, and install
#                        the chart with --set <comp>.image.tag=<tag>.
#   e.g. `make kind-up BUILD=sha-abc1234`
#        `make kind-up BUILD=v1.0.0-rc.3`

KIND_CLUSTER ?= mxl-k8s-demo
# Container runtime: "docker" (default) or "podman".
CONTAINER_RUNTIME ?= docker
# Image source: "local" (default) or a CI-produced image tag.
BUILD ?= local
# Where the integration suite writes failure diagnostics.
KIND_DIAG_DIR ?= $(CURDIR)/kind-diagnostics

.PHONY: kind-up
kind-up:
	KIND_CLUSTER=$(KIND_CLUSTER) CONTAINER_RUNTIME=$(CONTAINER_RUNTIME) BUILD=$(BUILD) bash hack/kind-up.sh

.PHONY: kind-down
kind-down:
	KIND_CLUSTER=$(KIND_CLUSTER) CONTAINER_RUNTIME=$(CONTAINER_RUNTIME) bash hack/kind-down.sh

.PHONY: kind-status
kind-status:
	KIND_CLUSTER=$(KIND_CLUSTER) CONTAINER_RUNTIME=$(CONTAINER_RUNTIME) bash hack/kind-status.sh

.PHONY: kind-test
kind-test:
	KIND_CLUSTER=$(KIND_CLUSTER) KIND_DIAG_DIR=$(KIND_DIAG_DIR) bash test/integration/kind/run.sh
