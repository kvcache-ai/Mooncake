"""Optional runtime integrations for Mooncake EPD.

Core state and control-plane modules must remain importable without a serving
runtime.  Version-sensitive integrations live below this package so callers
must opt in to them explicitly.
"""
