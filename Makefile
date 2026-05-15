CONTROLLER_TOOLS_VERSION ?= v0.18.0
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)
BUF_VERSION ?= v1.50.0
BUF ?= go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

MODULES := api ipc operator agent gateway

.PHONY: all
all: gen-api gen-ipc lint build

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

.PHONY: gen-ipc
gen-ipc:
	cd ipc && $(BUF) generate

CRD_GEN_PATHS := config/ api/v1alpha1/zz_generated.deepcopy.go
IPC_GEN_PATHS := ipc/v1

.PHONY: manifests-check
manifests-check: gen-api gen-rbac
	@if ! git diff --exit-code -- $(CRD_GEN_PATHS); then \
		echo "Generated CRD/DeepCopy/RBAC files are out of sync."; \
		echo "Run 'make gen-api gen-rbac' and commit the result."; \
		exit 1; \
	fi

.PHONY: ipc-check
ipc-check: gen-ipc
	@if ! git diff --exit-code -- $(IPC_GEN_PATHS); then \
		echo "Generated proto files are out of sync."; \
		echo "Run 'make gen-ipc' and commit the result."; \
		exit 1; \
	fi

.PHONY: generated-check
generated-check: manifests-check ipc-check
