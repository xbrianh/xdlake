import os
import unittest

import xdlake
from xdlake import delta_log


fixtures = os.path.abspath(os.path.join(os.path.dirname(__file__), "fixtures"))
logdir = os.path.join(fixtures, "_delta_log")


class TestDeltaLog(unittest.TestCase):
    def test_resolve_schema(self):
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
        s = dlog.resolve_schema()
        self.assertEqual(s, expected)


if __name__ == "__main__":
    unittest.main()
