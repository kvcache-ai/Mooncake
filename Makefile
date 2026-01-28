.PHONY: tidy
tidy:
	@find mooncake-transfer-engine -name "*.cpp" | head -5 | xargs -P $(shell nproc) -I {} clang-tidy-20 -p build --warnings-as-errors='*' {}
