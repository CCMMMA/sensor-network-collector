import logging
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import main
import signalk_access


class CollectorRobustnessTests(unittest.TestCase):
    def test_message_processing_errors_do_not_escape_callback(self):
        message = SimpleNamespace(topic="weather/station", payload=b'{"TempOut":20}')
        with patch("main._process_message", side_effect=RuntimeError("boom")), \
                self.assertLogs(main.logger, level="ERROR"):
            main.on_message(None, None, message)

    def test_signalk_url_disables_subscription_and_encodes_token(self):
        publisher = main.SignalKWebsocketPublisher("ws://host/signalk/v1/stream", "a b&c")
        self.assertEqual(publisher._build_url(), "ws://host/signalk/v1/stream?subscribe=none&token=a%20b%26c")
        publisher = main.SignalKWebsocketPublisher("ws://host/signalk/v1/stream?subscribe=self")
        self.assertEqual(publisher._build_url(), "ws://host/signalk/v1/stream?subscribe=self")

    def test_signalk_token_is_used_when_it_cannot_be_persisted(self):
        ready = Mock()
        manager = signalk_access.SignalKAccessManager(
            server_url="ws://host/signalk/v1/stream", initial_token="", config_path="/nonexistent/config.json",
            config_token_key="signalkToken", client_id="id", description="d", poll_interval_sec=1,
            request_timeout_sec=1, client_factory=Mock(), on_client_ready=ready,
            on_client_unavailable=Mock(), on_token_approved=Mock(), logger=logging.getLogger("test"),
        )
        with self.assertLogs("test", level="ERROR"):
            manager._approve_token("token")
        manager.on_token_approved.assert_called_once_with("token")
        ready.assert_called_once()


if __name__ == "__main__":
    unittest.main()
