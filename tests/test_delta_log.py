import os
import unittest
from uuid import uuid4
from unittest import mock

import xdlake
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
        dlog = xdlake.read_delta_log(LOGDIR)
        s = dlog.schema()
        self.assertEqual(s, expected)

    def test_replace(self):
        aa = delta_log.Add(
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
                tc = delta_log.DeltaLogEntry.commit_overwrite_table(expected_partition_columns, [], [])
                dlog = xdlake.read_delta_log(LOGDIR)
                dlog.entries[len(dlog.entries)] = tc
                self.assertEqual(expected_partition_columns, dlog.partition_columns())


if __name__ == "__main__":
    unittest.main()
