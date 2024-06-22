import os
import random
import shutil
import unittest
from uuid import uuid4
from tempfile import TemporaryDirectory

import numpy as np
import pyarrow as pa
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

    def __next__(self) -> pa.Table:
        t = pa.table(
            [np.random.random(11) for _ in range(len(self.columns))],
            names = self.columns,
        )

        order = [float(i) + self.order_parm for i in range(len(t))]
        self.order_parm += len(t)

        for name, choices in self.categoricals.items():
            t = t.append_column(name, [random.choice(choices) for _ in range(len(t))])
        t = t.append_column("order", pa.array(order, pa.float64()))
        return t

def _assert_arrow_table_equal(a, b):
    a = a.to_pandas().sort_values("order").sort_index(axis=1).reset_index(drop=True)
    b = b.to_pandas().sort_values("order").sort_index(axis=1).reset_index(drop=True)
    assert_frame_equal(a, b)

class TestXdLake(unittest.TestCase):
    def setUp(self):
        self.table_gen = TableGen()

    def write_table(self, loc, **kwargs) -> dict:
        if loc.startswith("s3://"):
            fs = xdlake.storage.get_filesystem("s3")
        else:
            fs = xdlake.storage.get_filesystem("file")
        t = next(self.table_gen)

        written_files = list()

        def visitor(visited_file):
            written_files.append(visited_file.path)

        pa.dataset.write_dataset(
            t,
            loc,
            format="parquet",
            filesystem=fs,
            file_visitor=visitor,
            **kwargs
        )

        return {"table": t, "written_files": written_files}

    def write_tables(self, locs, **kwargs):
        tables, paths = list(), list()
        for loc in locs:
            info = self.write_table(loc, **kwargs)
            tables.append(info["table"])
            paths.extend(info["written_files"])
        return tables, paths

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
        dl_loc = f"testdl/{uuid4()}"
        writer = xdlake.Writer(loc)

        tables = [next(self.table_gen) for _ in range(2)]
        self.table_gen.columns.append("new_column")
        table_new_schema = next(self.table_gen)

        for t in tables:
            writer.write(t, mode="append")

        with self.subTest("should raise"):
            with self.assertRaises(ValueError):
                writer.write(table_new_schema, mode="append")

        with self.subTest("should raise"):
            writer.write(table_new_schema, mode="append", schema_mode="merge")

        shutil.rmtree(dl_loc, ignore_errors=True)
        for t in tables:
            deltalake.write_deltalake(dl_loc, t, mode="append")
        deltalake.write_deltalake(dl_loc, table_new_schema, mode="append", schema_mode="merge", engine="rust")

        _assert_arrow_table_equal(
            deltalake.DeltaTable(dl_loc).to_pyarrow_table(),
            xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table(),
        )

    def test_remote_log(self):
        tables = [next(self.table_gen) for _ in range(3)]
        expected = pa.concat_tables(tables)
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
            pa.concat_tables(tables),
            xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table()
        )

    def test_write_kind(self):
        with TemporaryDirectory() as tempdir:
            tables, paths = self.write_tables([os.path.join(f"{tempdir}", f"{uuid4()}.parquet") for _ in range(27)])
            ds = pa.dataset.dataset(paths)
            expected = pa.concat_tables(tables)

            with self.subTest("write pyarrow dataset"):
                loc = f"testdl/{uuid4()}"
                writer = xdlake.Writer(loc)
                writer.write(ds, partition_by=["cats"])
                _assert_arrow_table_equal(expected, xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table())

            with self.subTest("write pyarrow record batches"):
                loc = f"testdl/{uuid4()}"
                writer = xdlake.Writer(loc)
                for batch in ds.to_batches():
                    writer.write(batch, partition_by=["cats"])
                _assert_arrow_table_equal(expected, xdlake.DeltaTable(loc).to_pyarrow_dataset().to_table())


if __name__ == '__main__':
    unittest.main()
