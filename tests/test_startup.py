import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import main


class StartupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = Path(self.temp.name) / "config.json"
        self.config.write_text(json.dumps({
            "mqttBroker": "localhost", "influxdb": False, "signalk": False,
            "storage": True, "pathStorage": str(Path(self.temp.name) / "storage"),
        }))

    def test_startup_with_config_and_mocked_mqtt(self):
        with patch("sys.argv", ["main.py", "--config", str(self.config)]), \
                patch("main.mqtt.Client") as client, \
                patch("main.signal.signal"), patch.dict(main.runtime):
            main.main()
            client.return_value.connect.assert_called_once_with("localhost", 1883)
            client.return_value.loop_forever.assert_called_once()

    def test_legacy_web_settings_do_not_stop_the_collector(self):
        raw = json.loads(self.config.read_text())
        raw.update(httpEnabled=True, smtpEnabled=True, adminPassword="admin")
        self.config.write_text(json.dumps(raw))
        with patch("sys.argv", ["main.py", "--config", str(self.config), "--http", "true"]), \
                patch("main.mqtt.Client") as client, patch("main.signal.signal"), \
                patch.dict(main.runtime), self.assertLogs(main.logger, level="WARNING"):
            main.main()
            client.return_value.loop_forever.assert_called_once()
        self.assertFalse(hasattr(main, "create_web_app"))


if __name__ == "__main__":
    unittest.main()
