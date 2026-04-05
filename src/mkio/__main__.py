"""CLI entry point: mkio serve <config.toml>"""

import sys

from mkio.server import serve


def main() -> None:
    if len(sys.argv) < 3 or sys.argv[1] != "serve":
        print("Usage: mkio serve <config.toml>")
        sys.exit(1)
    serve(sys.argv[2])


if __name__ == "__main__":
    main()
