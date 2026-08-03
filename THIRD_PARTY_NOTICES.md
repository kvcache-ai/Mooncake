# Third-Party Notices

This document lists third-party source code included in this repository,
including project name, upstream repository, fixed commit, copyright,
license, local path, and whether Hygon has modified the component.

The project itself is derived from upstream Mooncake
(https://github.com/kvcache-ai/Mooncake), Commit
`6041a609a8c3af35e778f70db344f145c2914980`, licensed under Apache-2.0.

## Summary

| Component | Local path | License | Fixed Commit |
| --- | --- | --- | --- |
| pybind11 | `extern/pybind11/` | BSD-3-Clause | `58c382a8e3d7081364d2f5c62e7f429f0412743b` |
| yalantinglibs | `extern/yalantinglibs/` | Apache-2.0 (see subcomponent licenses below) | `73dea196d23ad8fcd4914c6ef1238f390b9a1c48` |

These directories were originally Git submodules of upstream Mooncake and were
converted to regular in-tree directories for offline development. Treat each
tree as a third-party component as a whole; do not reinterpret unmodified
files as Hygon-original source.

---

## pybind11

- **Project**: pybind11
- **Upstream repository**: https://github.com/pybind/pybind11
- **Fixed Commit**: `58c382a8e3d7081364d2f5c62e7f429f0412743b`
- **Version**: 2.13.6
- **Copyright**: Copyright (c) 2016 Wenzel Jakob \<wenzel.jakob@epfl.ch\>, All rights reserved. Additional copyrights appear in individual source files as noted by their authors.
- **License**: BSD-3-Clause (see `extern/pybind11/LICENSE`)
- **Local path**: `extern/pybind11/`
- **Hygon modifications**: No functional source modifications.

---

## yalantinglibs

- **Project**: yaLanTingLibs
- **Upstream repository**: https://github.com/alibaba/yalantinglibs
- **Fixed Commit**: `73dea196d23ad8fcd4914c6ef1238f390b9a1c48`
- **Version**: 0.5.7
- **Copyright**: Copyright holders as stated in the upstream project and
  individual source files (see `extern/yalantinglibs/LICENSE` and file headers).
- **License**: Apache-2.0 (see `extern/yalantinglibs/LICENSE`)
- **Local path**: `extern/yalantinglibs/`
- **Hygon modifications**: No functional source modifications.

### yalantinglibs subcomponent licenses

As stated in `extern/yalantinglibs/NOTICE`, the following third-party
subcomponents are included under additional licenses. Use of these paths is
subject to the listed licenses and requires legal/compliance approval where
applicable.

| License | Upstream NOTICE path | Local path in this tree |
| --- | --- | --- |
| Apache-2.0 | `ylt/struct_pack/texpr.hpp` | As listed in `extern/yalantinglibs/NOTICE` under Apache Software Foundation License 2.0 |
| BSD-3-Clause | `util/expected.hpp` | `extern/yalantinglibs/include/ylt/util/expected.hpp` |
| CC0-1.0 | `test/doctest.h` | `extern/yalantinglibs/src/include/doctest.h` |
| CC0-1.0 | `include/ylt/struct_pack/pp` | As listed in upstream NOTICE; may be absent or restructured in this fixed Commit — treat NOTICE entry as authoritative for license scope |
| BSL-1.0 | Boost Software License components in upstream NOTICE | See `extern/yalantinglibs/NOTICE` (Boost Software License, Version 1.0) |
