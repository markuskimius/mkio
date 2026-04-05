"""Programmatic example: start mkio with custom expression functions."""

from mkio import serve, register_function

# Register custom expression functions before starting
register_function("DOUBLE", lambda x: x * 2)
register_function("STATUS_EMOJI", lambda s: {"pending": "⏳", "filled": "✅", "cancelled": "❌"}.get(s, "❓"))

serve("config.toml")
