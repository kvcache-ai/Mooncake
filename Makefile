# Mooncake 快速 K8s 部署
# 用法: make <target>
# 所有命令最终部署到 K8s 集群

.PHONY: help deploy-operator deploy-ui deploy-store deploy-all \
        setup-ccache build-cpp reconfigure-ninja

help:
	@echo "Mooncake 快速 K8s 部署"
	@echo ""
	@echo "用法: make <target>"
	@echo ""
	@echo "--- 部署到 K8s（修改什么就部署什么） ---"
	@echo "  deploy-operator  改 Go  → 编译 → Docker → kind → 重启 (~30s)"
	@echo "  deploy-ui        改 UI  → build → Docker → kind → 重启 (~1min)"
	@echo "  deploy-store     改 C++ → 编译 → wheel → Docker → kind → 重启 (~3min)"
	@echo "  deploy-all       全部组件一次部署"
	@echo ""
	@echo "--- 辅助 ---"
	@echo "  setup-ccache      配置 C++ 编译缓存（一次性的，已执行）"
	@echo "  build-cpp [TARGET=] 纯编译 C++（不部署，用于检查）"
	@echo "  reconfigure-ninja 切换到 Ninja 构建系统"
	@echo ""
	@echo "详情: bash scripts/dev-cycle.sh"

deploy-operator: ## Go 改完后快速部署到 K8s
	bash scripts/dev-cycle.sh deploy-operator

deploy-ui: ## UI 改完后快速部署到 K8s
	bash scripts/dev-cycle.sh deploy-ui

deploy-store: ## C++ 改完后快速部署到 K8s
	bash scripts/dev-cycle.sh deploy-store

deploy-all: ## 全部组件部署到 K8s
	bash scripts/dev-cycle.sh deploy-all

setup-ccache: ## 配置 C++ 编译缓存
	bash scripts/dev-cycle.sh setup-ccache

build-cpp: ## 纯增量编译 C++（make build-cpp ARGS="mooncake-store"）
	bash scripts/dev-cycle.sh build-cpp $(ARGS)

reconfigure-ninja: ## 切换到 Ninja 构建
	bash scripts/dev-cycle.sh reconfigure-ninja
