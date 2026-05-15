CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen
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
	$(CONTROLLER_GEN) object paths=./api/...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:dir=./config/crd
	$(CONTROLLER_GEN) rbac:roleName=mxl-k8s-operator paths=./operator/... output:rbac:dir=./config/rbac

.PHONY: gen-ipc
gen-ipc:
	cd ipc && $(BUF) generate

.PHONY: manifests-check
manifests-check: gen-api
	@git diff --exit-code -- config/ || \
		(echo "config/ is out of sync with controller-gen output; run 'make gen-api'"; exit 1)
