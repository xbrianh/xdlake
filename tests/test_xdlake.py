import shutil
import random
import unittest
from uuid import uuid4

import numpy as np
import pyarrow
import deltalake
from pandas.testing import assert_frame_equal

import xdlake


class TableGen:
    def __init__(self, columns=["bob", "sue", "george", "rebecca", "morgain"]):
        self.columns = columns
        self.categoricals = {
            "cats": ["S", "A", "D"],
            "bats": ["F", "G", "H"],
        }
        self.order_parm = 0

    def __next__(self) -> pyarrow.Table:
        t = pyarrow.table(
            [np.random.random(11) for _ in range(len(self.columns))],
            names = self.columns,
        )

        order = [float(i) + self.order_parm for i in range(len(t))]
        self.order_parm += len(t)

        for name, choices in self.categoricals.items():
            t = t.append_column(name, [random.choice(choices) for _ in range(len(t))])
        t = t.append_column("order", pyarrow.array(order, pyarrow.float64()))
        return t

def _assert_arrow_table_equal(a, b):
    a = a.to_pandas().sort_values("order").sort_index(axis=1).reset_index(drop=True)
    b = b.to_pandas().sort_values("order").sort_index(axis=1).reset_index(drop=True)
    assert_frame_equal(a, b)

class TestXdLake(unittest.TestCase):
    def setUp(self):
        self.table_gen = TableGen()

    def test_xdlake(self):
        loc = f"testdl/{uuid4()}"
        writer = xdlake.Writer(loc)

        for _ in range(3):
            t = next(self.table_gen)
            writer.write(t, partition_by=["cats", "bats"])

        with self.subTest("should aggree", mode="append"):
            df_expected = deltalake.DeltaTable(loc)
            df = xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
            _assert_arrow_table_equal(df_expected, df)

        with self.subTest("should aggree", mode="overwrite"):
            t = next(self.table_gen)
            writer.write(t, partition_by=["cats", "bats"], mode="overwrite")
            df_expected = deltalake.DeltaTable(loc)
            df = xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
            _assert_arrow_table_equal(df_expected, df)

    def test_schema_change(self):
        loc = f"testdl/{uuid4()}"
        writer = xdlake.Writer(loc)

        for _ in range(2):
            t = next(self.table_gen)
            writer.write(t, mode="append")

        self.table_gen.columns.append("new_column")
        with self.assertRaises(ValueError):
            writer.write(next(self.table_gen), mode="append")

    def test_remote_log(self):
        tables = [next(self.table_gen) for _ in range(3)]
        expected = pyarrow.concat_tables(tables)
        tests = [
            (f"s3://test-xdlake/tests/{uuid4()}", f"testdl/{uuid4()}"),
            (f"testdl/{uuid4()}", f"s3://test-xdlake/tests/{uuid4()}"),
        ]
        for data_loc, log_loc in tests:
            with self.subTest(data_loc=data_loc, log_loc=log_loc):
                writer = xdlake.Writer(data_loc, log_loc)

                for t in tables:
                    writer.write(t)

                _assert_arrow_table_equal(
                    expected,
                    xdlake.DeltaTable(data_loc, log_loc).to_pyarrow_dataset().to_table(),
                )

    def test_write_mode_error_ignore(self):
        loc = f"testdl/{uuid4()}"
        writer = xdlake.Writer(loc)
        expected = next(self.table_gen)
        writer.write(expected)

        with self.subTest("should raise FileExistsError"):
            with self.assertRaises(FileExistsError):
                writer.write(next(self.table_gen), mode="error")
            df = xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
            _assert_arrow_table_equal(expected, df)

        with self.subTest("should not write to table, and not raise"):
            writer.write(next(self.table_gen), mode="ignore")
            df = xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
            _assert_arrow_table_equal(expected, df)

    def test_s3(self):
        loc = f"s3://test-xdlake/tests/{uuid4()}"
        writer = xdlake.Writer(loc)
        tables = [next(self.table_gen) for _ in range(3)]

        for t in tables:
            writer.write(t, partition_by=["cats"])

        _assert_arrow_table_equal(
            pyarrow.concat_tables(tables),
            xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
        )

    def write_deltalake(self):
        loc = "tdl"
        shutil.rmtree(loc, ignore_errors=True)

        for _ in range(1):
            deltalake.write_deltalake("tdl", next(self.table_gen), mode="append")

        for _ in range(1):
            deltalake.write_deltalake("tdl", next(self.table_gen), mode="overwrite")

    def test_foo(self):
        a = deltalake.DeltaTable("tdl").to_pyarrow_dataset().to_table().to_pandas()
        b = xdlake.DeltaTable("tdl").to_pyarrow_dataset().to_table().to_pandas()
        assert_frame_equal(a, b)


if __name__ == '__main__':
    unittest.main()
