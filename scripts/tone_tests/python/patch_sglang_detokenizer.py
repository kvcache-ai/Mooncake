# SPDX-License-Identifier: Apache-2.0

from importlib.util import find_spec
from pathlib import Path


def main() -> None:
    spec = find_spec("sglang")
    if spec is None or spec.submodule_search_locations is None:
        raise RuntimeError("Cannot locate the installed sglang package")

    source_path = (
        Path(next(iter(spec.submodule_search_locations)))
        / "srt"
        / "managers"
        / "schedule_batch.py"
    )
    source = source_path.read_text()
    old = "self.origin_input_ids_unpadded[self.surr_offset :] + output_ids"
    new = "list(self.origin_input_ids_unpadded[self.surr_offset :]) + list(output_ids)"

    if new in source:
        print(f"SGLang detokenizer patch already applied: {source_path}")
        return
    if source.count(old) != 1:
        raise RuntimeError(
            f"Expected one SGLang detokenizer patch point in {source_path}, "
            f"found {source.count(old)}"
        )

    source_path.write_text(source.replace(old, new))
    print(f"Patched SGLang detokenizer list concatenation: {source_path}")


if __name__ == "__main__":
    main()
