# Staged as the wheel's top-level tent package; kept out of python/ so a source
# checkout never shadows the build-tree extension installed as mooncake.tent.
import mooncake.tent as _tent

__all__ = [name for name in dir(_tent) if not name.startswith("_")]
globals().update({name: getattr(_tent, name) for name in __all__})
