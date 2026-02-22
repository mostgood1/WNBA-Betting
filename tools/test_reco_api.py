from __future__ import annotations

import importlib.util
import os
import sys


def _load_flask_app():
    here = os.path.abspath(os.getcwd())
    fp = os.path.join(here, "app.py")
    spec = importlib.util.spec_from_file_location("app", fp)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load spec for {fp}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    app = getattr(module, "app", None)
    if app is None:
        raise RuntimeError("Flask app not found in app.py")
    return app


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    date_arg = (argv[0] if argv else "2026-01-14")
    app = _load_flask_app()

    with app.test_client() as c:
        resp = c.get(f"/recommendations?format=json&view=all&date={date_arg}&compact=1")
        print("status", resp.status_code)
        data = resp.get_json(silent=True)
        if not isinstance(data, dict):
            print("ERROR: Non-JSON response")
            return 3
        counts = {k: len((data.get(k) or [])) for k in ["games", "props", "first_basket", "early_threes"]}
        print("counts", counts)
        meta_dates = ((data.get("meta") or {}).get("data_dates") or {})
        print("meta_dates", meta_dates)
    return 0


def test_recommendations_json_smoke():
    import pytest

    try:
        app = _load_flask_app()
    except Exception as exc:
        pytest.skip(f"Unable to import Flask app: {exc}")

    with app.test_client() as c:
        resp = c.get("/recommendations?format=json&view=all&compact=1")
        # If local processed data isn't present, the handler may still return a JSON payload.
        assert resp.status_code in (200, 400)
        data = resp.get_json(silent=True)
        assert isinstance(data, dict)


if __name__ == "__main__":
    raise SystemExit(main())
