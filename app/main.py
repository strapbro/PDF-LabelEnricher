from __future__ import annotations

import os

import uvicorn


def main() -> None:
    port = int(os.environ.get("LABEL_ENRICHER_PORT", "8081"))
    uvicorn.run("app.ui_server:app", host="127.0.0.1", port=port, reload=False)


if __name__ == "__main__":
    main()
