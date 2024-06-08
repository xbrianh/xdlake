import os
import shutil
import random
import unittest
from uuid import uuid4

import numpy as np
import pyarrow
import deltalake

import xdlake


cats = ["S", "A", "D"]

class TestXdLake(unittest.TestCase):
    def test_xdlake(self):
        test_dir = f"testdl/{uuid4()}"

        for _ in range(4):
            t = pyarrow.table(
                [np.random.random(11) for _ in range(5)],
                names = ["bob", "sue", "george", "rebecca", "morgain"],
            )
            t = t.append_column("cats", [random.choice(cats) for _ in range(len(t))])
            xdlake.write(test_dir, t, partition_by=["cats"])

        t = deltalake.DeltaTable(test_dir)
        t.to_pandas()

    def test_xdlake_s3(self):
        test_dir = f"s3://test-xdlake/tests/{uuid4()}"

        for _ in range(4):
            t = pyarrow.table(
                [np.random.random(11) for _ in range(5)],
                names = ["bob", "sue", "george", "rebecca", "morgain"],
            )
            t = t.append_column("cats", [random.choice(cats) for _ in range(len(t))])
            xdlake.write(test_dir, t, partition_by=["cats"])

        # t = deltalake.DeltaTable("testdl")
        # t.to_pandas()

    def test_storage(self):
        name = f"{uuid4()}"
        tests = [
            ("/tmp/tests", f"/tmp/tests/foo/{name}"),
            ("tmp/tests", f"{os.getcwd()}/tmp/tests/foo/{name}"),
            ("file:///tmp/tests", f"/tmp/tests/foo/{name}"),
            ("s3://test-xdlake/tests", f"s3://test-xdlake/tests/foo/{name}"), 
        ]
        for url, expected_path in tests:
            loc = xdlake.StorageLocation(url)
            p = loc.append_path("foo", name)
            self.assertEqual(p, expected_path)
            d = os.urandom(8)
            with loc.open(p, mode="wb") as fh:
                fh.write(d)
            with loc.open(p, mode="rb") as fh:
                self.assertEqual(fh.read(), d)

    def write_deltalake(self):
        test_dir = "tdl"
        shutil.rmtree(test_dir, ignore_errors=True)

        for _ in range(4):
            t = pyarrow.table(
                [np.random.random(11) for _ in range(5)],
                names = ["bob", "sue", "george", "rebecca", "morgain"],
            )
            deltalake.write_deltalake("tdl", t, mode="append")


if __name__ == '__main__':
    unittest.main()
