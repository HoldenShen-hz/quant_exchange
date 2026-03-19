"""Quant Exchange MVP package."""

__all__ = ["QuantTradingPlatform"]


def __getattr__(name: str):
    """Load top-level exports lazily to keep submodule CLIs lightweight."""

    if name == "QuantTradingPlatform":
        from .platform import QuantTradingPlatform

        return QuantTradingPlatform
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
