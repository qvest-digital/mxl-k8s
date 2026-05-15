CONTROLLER_TOOLS_VERSION ?= v0.18.0
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)
BUF ?= buf

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

.PHONY: gen-ipc
gen-ipc:
	cd ipc && $(BUF) generate

GENERATED_PATHS := config/ api/v1alpha1/zz_generated.deepcopy.go

.PHONY: manifests-check
manifests-check: gen-api
	@if ! git diff --exit-code -- $(GENERATED_PATHS); then \
		echo "Generated files are out of sync with controller-gen output."; \
		echo "Run 'make gen-api' and commit the result."; \
		exit 1; \
	fi
