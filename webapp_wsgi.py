import os

from main import AccessStore, create_web_app, load_config


class _Args:
    def __init__(self):
        self.signalk = None
        self.influxdb = None
        self.storage = None
        self.http = None
        self.dry = False


def create_app(config_path: str | None = None):
    cfg_path = config_path or os.getenv("COLLECTOR_CONFIG", "config.json")
    cfg = load_config(cfg_path, _Args())

    if not cfg.get("enable_http"):
        raise RuntimeError("httpEnabled must be true for WSGI web deployment")

    access_store = AccessStore(cfg["auth_db_path"])
    access_store.ensure_admin(cfg["admin_user"], cfg["admin_password"])

    return create_web_app(cfg, access_store)


app = create_app()
