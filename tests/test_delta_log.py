import os
import unittest

import xdlake
from xdlake import delta_log, utils


fixtures = os.path.abspath(os.path.join(os.path.dirname(__file__), "fixtures"))
logdir = os.path.join(fixtures, "_delta_log")


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
        dlog = xdlake.read_delta_log(logdir)
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


if __name__ == "__main__":
    unittest.main()
