IMG ?= mooncake-operator:latest

.PHONY: all
all: build

##@ General

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: ## Generate CRD manifests
	$(CONTROLLER_GEN) crd paths="./..." output:crd:dir=config/crd/bases

.PHONY: generate
generate: ## Generate DeepCopy methods
	$(CONTROLLER_GEN) object paths="./..."

.PHONY: fmt
fmt: ## Run go fmt
	go fmt ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

##@ Build

.PHONY: build
build: fmt vet ## Build operator binary
	go build -o bin/manager ./cmd/main.go

.PHONY: run
run: fmt vet ## Run operator locally
	go run ./cmd/main.go

.PHONY: docker-build
docker-build: ## Build docker image
	docker build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image
	docker push ${IMG}

##@ Deployment

.PHONY: install
install: manifests ## Install CRDs into cluster
	kubectl apply -f config/crd/bases/

.PHONY: uninstall
uninstall: ## Uninstall CRDs from cluster
	kubectl delete -f config/crd/bases/

.PHONY: deploy
deploy: manifests ## Deploy operator to cluster
	kubectl apply -f config/rbac/
	kubectl apply -f config/manager/

.PHONY: undeploy
undeploy: ## Remove operator from cluster
	kubectl delete -f config/manager/
	kubectl delete -f config/rbac/

CONTROLLER_GEN = $(shell which controller-gen 2>/dev/null)
