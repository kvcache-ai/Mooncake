"""Package staging directory for the root Mooncake Python distribution.

``scripts/build_wheel.sh`` stages ``python/mooncake/__init__.py`` here before
building. The root package owns its import behavior and Store backend facade.
"""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
