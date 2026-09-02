import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import main


class StartupAndLoginTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = Path(self.temp.name) / "config.json"
        self.config.write_text(json.dumps({
            "mqttBroker": "localhost", "influxdb": False, "signalk": False,
            "storage": True, "pathStorage": str(Path(self.temp.name) / "storage"),
            "httpEnabled": False, "webSessionSecret": "test-only-secret",
        }))

    def test_startup_with_config_and_mocked_mqtt(self):
        with patch("sys.argv", ["main.py", "--config", str(self.config)]), \
                patch("main.mqtt.Client") as client, \
                patch("main.signal.signal"), patch.dict(main.runtime):
            main.main()
            client.return_value.connect.assert_called_once_with("localhost", 1883)
            client.return_value.loop_forever.assert_called_once()

    def test_login_redirects_only_to_local_paths(self):
        args = SimpleNamespace(storage=None, influxdb=None, signalk=None, http=None, dry=False)
        cfg = main.load_config(str(self.config), args)
        store = Mock()
        store.authenticate.return_value = {"username": "tester", "force_password_change": 0}
        with patch.dict(main.runtime):
            app = main.create_web_app(cfg, store)
            client = app.test_client()
            for target in ("https://example.com", "//example.com", "/\\example.com", "/\t/example.com"):
                with self.subTest(target=target):
                    response = client.post("/login", query_string={"next": target},
                                           data={"username": "tester", "password": "test"})
                    self.assertEqual(response.status_code, 302)
                    self.assertEqual(response.headers["Location"], "/")
            response = client.post("/login", query_string={"next": "/?window=day"},
                                   data={"username": "tester", "password": "test"})
            self.assertEqual(response.headers["Location"], "/?window=day")


if __name__ == "__main__":
    unittest.main()
