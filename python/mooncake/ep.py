import importlib

try:
    backend_module = importlib.import_module("mooncake._ep")
except ModuleNotFoundError:
    raise ImportError(
        "Mooncake EP was not built. Please rebuild Mooncake with WITH_EP=ON.\n"
        "Open an issue at https://github.com/kvcache-ai/Mooncake/issues."
    )
globals().update(
    {k: v for k, v in backend_module.__dict__.items() if not k.startswith("_")}
)
