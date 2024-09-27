import os
import unittest
from uuid import uuid4
from unittest import mock
from tempfile import TemporaryDirectory
from dataclasses import fields
from contextlib import contextmanager

from xdlake import delta_log, utils


fixtures = os.path.abspath(os.path.join(os.path.dirname(__file__), "fixtures"))
LOGDIR = os.path.join(fixtures, "_delta_log")


class TestDeltaLog(unittest.TestCase):
    def test_schema(self):
        expected = delta_log.Schema(
            fields=[
                {"name": "bob", "type": "double", "nullable": True, "metadata": {}},
                {"name": "sue", "type": "double", "nullable": True, "metadata": {}},
                {"name": "george", "type": "double", "nullable": True, "metadata": {}},
                {"name": "rebecca", "type": "double", "nullable": True, "metadata": {}},
                {"name": "morgain", "type": "double", "nullable": True, "metadata": {}},
                {"name": "cats", "type": "string", "nullable": True, "metadata": {}},
                {"name": "bats", "type": "string", "nullable": True, "metadata": {}},
                {"name": "order", "type": "double", "nullable": True, "metadata": {}},
                {
                    "name": "new_column",
                    "type": "double",
                    "nullable": True,
                    "metadata": {},
                },
            ],
            type="struct",
        )
        dlog = delta_log.DeltaLog.with_location(LOGDIR)
        s = dlog.schema()
        self.assertEqual(s, expected)

    def test_replace(self):
        aa = delta_log.actions.Add(
            path="this_is_fake",
            modificationTime=utils.timestamp(),
            size=1,
            stats={},
            partitionValues=dict(),
        )
        new_path = "this is still fake"
        new_size = 17
        new_aa = aa.replace(path=new_path, size=new_size)
        self.assertEqual(new_path, new_aa.path)
        self.assertEqual(new_size, new_aa.size)

    def test_partition_columns(self):
        for expected_partition_columns in (list(), [f"{uuid4()}" for _ in range(3)]):
            with self.subTest(expected=expected_partition_columns):
                tc = delta_log.DeltaLogEntry.overwrite_table(expected_partition_columns, [], [])
                dlog = delta_log.DeltaLog.with_location(LOGDIR)
                dlog.entries[len(dlog.entries)] = tc
                self.assertEqual(expected_partition_columns, dlog.partition_columns())

    def test_commit(self):
        obj = mock.MagicMock()

        @contextmanager
        def commit_ctx(loc):
            try:
                obj(loc.path)
                yield
            finally:
                pass

        with TemporaryDirectory() as scratch:
            dlog = delta_log.DeltaLog.with_location(f"{scratch}/_delta_log")
            write_loc = dlog.loc.append_path("00000000000000000000.json")
            with write_loc.open(mode="w") as fh:
                fh.write("")
            obj.assert_not_called()
            dlog.commit(mock.MagicMock(), commit_ctx)
            obj.assert_called_once_with(write_loc.path)

    def test_load_as_version(self):
        dlog = delta_log.DeltaLog("loc")
        expected_entries = [f"{uuid4()}" for _ in range(5)]
        for i, entry in enumerate(expected_entries):
            dlog[i] = entry

        for i in reversed(range(5)):
            foo = dlog.load_as_version(i)
            for v in foo.versions:
                assert foo[v] == expected_entries[v]

        with mock.patch("xdlake.delta_log.DeltaLog.load_as_version") as mock_load_as_version:
            dlog.load_as_version(0).load_as_version()
            mock_load_as_version.assert_called_once()

    def test_delta_log_action_attrs(self):
        for _t in delta_log.actions.DeltaLogAction.__subclasses__():
            self.assertTrue("extra_info" in {f.name for f in fields(_t)})


if __name__ == "__main__":
    unittest.main()
