from __future__ import annotations

import os
import sys

import uvicorn


def main() -> None:
    from app.ui_server import app as fastapi_app

    port = int(os.environ.get("LABEL_ENRICHER_PORT", "8080"))
    run_kwargs: dict[str, object] = {}
    # Windowed/frozen builds can have stdout/stderr as None; uvicorn's default
    # formatter expects a TTY stream and crashes on .isatty().
    if getattr(sys, "frozen", False) and (sys.stdout is None or sys.stderr is None):
        run_kwargs["log_config"] = None
    uvicorn.run(fastapi_app, host="127.0.0.1", port=port, reload=False, **run_kwargs)


if __name__ == "__main__":
    main()


