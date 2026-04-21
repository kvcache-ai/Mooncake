YA_LT_COPT = [
    "-fno-tree-slp-vectorize",  # -ftree-slp-vectorize with coroutine cause link error. disable it util gcc fix.
]

YA_BIN_COPT = [
    "-fno-tree-slp-vectorize",  # -ftree-slp-vectorize with coroutine cause link error. disable it util gcc fix.
    "-Wno-unused-but-set-variable",
    "-Wno-unused-value",
    "-Wno-unused-variable",
    "-Wno-sign-compare",
    "-Wno-reorder",
    "-Wno-unused-local-typedefs",
    "-Wno-missing-braces",
    "-Wno-uninitialized",
]
