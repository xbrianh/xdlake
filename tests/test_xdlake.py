import os
import shutil
import random
import unittest
from uuid import uuid4

import numpy as np
import pyarrow
import deltalake
from pandas.testing import assert_frame_equal

import xdlake


def pyarrow_table_gen() -> pyarrow.Table:
    order_parm = 0
    while True:
        cats = ["S", "A", "D"]
        bats = ["F", "G", "H"]
        t = pyarrow.table(
            [np.random.random(11) for _ in range(5)],
            names = ["bob", "sue", "george", "rebecca", "morgain"],
        )

        order = [float(i) + order_parm for i in range(len(t))]
        order_parm += len(t)

        t = t.append_column("cats", [random.choice(cats) for _ in range(len(t))])
        t = t.append_column("bats", [random.choice(bats) for _ in range(len(t))])
        t = t.append_column("order", pyarrow.array(order, pyarrow.float64()))
        yield t

def _assert_arrow_table_equal(a, b):
    a = a.to_pandas().sort_values("order").sort_index(axis=1).reset_index(drop=True)
    b = b.to_pandas().sort_values("order").sort_index(axis=1).reset_index(drop=True)
    assert_frame_equal(a, b)

class TestXdLake(unittest.TestCase):
    def setUp(self):
        self.table_gen = pyarrow_table_gen()

    def test_xdlake(self):
        test_dir = f"testdl/{uuid4()}"
        writer = xdlake.Writer(test_dir)

        for _ in range(3):
            t = next(self.table_gen)
            writer.write(t, partition_by=["cats", "bats"])

        with self.subTest("should aggree", mode="append"):
            df_expected = deltalake.DeltaTable(test_dir)
            df = xdlake.DeltaTable(test_dir).to_pyarrow_dataset().to_table()
            _assert_arrow_table_equal(df_expected, df)

        with self.subTest("should aggree", mode="overwrite"):
            t = next(self.table_gen)
            writer.write(t, partition_by=["cats", "bats"], mode="overwrite")
            df_expected = deltalake.DeltaTable(test_dir)
            df = xdlake.DeltaTable(test_dir).to_pyarrow_dataset().to_table()
            _assert_arrow_table_equal(df_expected, df)

    def test_xdlake_s3(self):
        test_dir = f"s3://test-xdlake/tests/{uuid4()}"
        writer = xdlake.Writer(test_dir)

        for _ in range(4):
            t = next(self.table_gen)
            writer.write(t, partition_by=["cats"])

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

        for _ in range(1):
            t = next(self.table_gen)
            deltalake.write_deltalake("tdl", t, mode="append")

        for _ in range(1):
            deltalake.write_deltalake("tdl", t, mode="overwrite")

    def test_foo(self):
        a = deltalake.DeltaTable("tdl").to_pyarrow_dataset().to_table().to_pandas()
        b = xdlake.DeltaTable("tdl").to_pyarrow_dataset().to_table().to_pandas()
        assert_frame_equal(a, b)


if __name__ == '__main__':
    unittest.main()
