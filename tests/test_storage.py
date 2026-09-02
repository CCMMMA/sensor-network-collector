import csv
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import main


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "storage"
        self.storage = main.CSVHourlyStorage(str(self.root))
        self.dt = datetime(2026, 9, 3, 10, tzinfo=timezone.utc)

    def test_rejects_unsafe_identifiers_before_creating_files(self):
        for identifier in ("../outside", "/tmp/outside", "a/b", "a\\b", ".", "..", "", "\x00"):
            with self.subTest(identifier=identifier), self.assertRaises(ValueError):
                self.storage.write(identifier, self.dt, {"temperature": 20})
        self.assertFalse(self.root.exists())

    def test_rejects_symlink_escape(self):
        self.root.mkdir()
        outside = Path(self.temp.name) / "outside"
        outside.mkdir()
        (self.root / "station").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.storage.write("station", self.dt, {"temperature": 20})
        self.assertEqual(list(outside.iterdir()), [])

    def test_schema_expansion_preserves_old_rows_and_permissions(self):
        path = Path(self.storage.write("station", self.dt, {"temperature": 20}))
        path.chmod(0o640)
        self.storage.write("station", self.dt, {"temperature": 21, "humidity": 60})
        with path.open(newline="") as stream:
            self.assertEqual(list(csv.DictReader(stream)), [
                {"temperature": "20", "humidity": ""},
                {"temperature": "21", "humidity": "60"},
            ])
        self.assertEqual(path.stat().st_mode & 0o777, 0o640)

    def test_failed_schema_replacement_preserves_original_and_cleans_temp(self):
        path = Path(self.storage.write("station", self.dt, {"temperature": 20}))
        original = path.read_bytes()
        with patch("main.os.replace", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                self.storage.write("station", self.dt, {"humidity": 60})
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(list(path.parent.iterdir()), [path])

    def test_payload_cannot_overwrite_collector_metadata(self):
        message = SimpleNamespace(topic="weather/station", payload=(
            b'{"uuid":" station ","timestamp":"spoofed","topic":"spoofed",'
            b'"Datetime":"2026-09-03T10:00:00Z","temperature":20}'
        ))
        cfg = dict(skip_empty_fields=True, enable_storage=True, enable_influx=False,
                   enable_signalk=False, dry=False)
        with patch.dict(main.runtime, config=cfg, csv_storage=self.storage):
            main.on_message(None, None, message)
        path = self.storage._file_path("station", self.dt)
        with path.open(newline="") as stream:
            row = next(csv.DictReader(stream))
        self.assertEqual(row["timestamp"], "2026-09-03T10:00:00Z")
        self.assertEqual(row["topic"], "weather/station")
        self.assertEqual(row["uuid"], "station")

    def test_invalid_storage_identifier_does_not_stop_other_sinks(self):
        message = SimpleNamespace(topic="weather/station", payload=b'{"uuid":"../outside","TempOut":20}')
        for dry in (False, True):
            cfg = dict(skip_empty_fields=True, enable_storage=True, enable_influx=False,
                       enable_signalk=True, dry=dry)
            build_delta = Mock(return_value=None)
            with patch.dict(main.runtime, config=cfg, csv_storage=self.storage), \
                    patch("main.build_signalk_delta", build_delta), \
                    self.assertLogs(main.logger, level="ERROR"):
                main.on_message(None, None, message)
            build_delta.assert_called_once()


if __name__ == "__main__":
    unittest.main()
