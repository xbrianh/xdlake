import unittest
from uuid import uuid4

import deltalake
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pandas.testing import assert_frame_equal

import xdlake

from tests.utils import TableGenMixin, assert_arrow_table_equal


class TestCompatibility(TableGenMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.partition_by = list(self.table_gen.categoricals.keys())

    def test_append_and_overwrite(self):
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        dt_loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(3)]
        overwrite_arrow_table = self.gen_table()

        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=self.partition_by)
            deltalake.write_deltalake(dt_loc, at, partition_by=self.partition_by, mode="append")

        with self.subTest("should aggree", mode="append"):
            assert_arrow_table_equal(deltalake.DeltaTable(xdl.loc.path), xdlake.DeltaTable(dt_loc).to_pyarrow_table())

        with self.subTest("should aggree", mode="overwrite"):
            xdl = xdl.write(overwrite_arrow_table, partition_by=self.partition_by, mode="overwrite")
            deltalake.write_deltalake(dt_loc, overwrite_arrow_table, partition_by=self.partition_by, mode="overwrite")
            assert_arrow_table_equal(deltalake.DeltaTable(xdl.loc.path), xdlake.DeltaTable(dt_loc).to_pyarrow_table())

    def test_custom_metadata(self):
        xdl_loc = f"{self.scratch_folder}/{uuid4()}"
        dt_loc = f"{self.scratch_folder}/{uuid4()}"
        xdl = xdlake.DeltaTable(xdl_loc)
        for _ in range(1):
            custom_metadata = {"foo": f"{uuid4()}", "bar": f"{uuid4()}"}
            at = self.gen_table()
            xdl = xdl.write(at, partition_by=self.partition_by, custom_metadata=custom_metadata)
            deltalake.write_deltalake(dt_loc, at, partition_by=self.partition_by, mode="append", custom_metadata=custom_metadata)

        with self.subTest("direct"):
            hist_a = deltalake.DeltaTable(dt_loc).history()
            hist_b = xdlake.DeltaTable(xdl_loc).history()
            for a, b in zip(hist_a, hist_b):
                self.assertEqual(a["version"], b["version"])
                for k in ["foo", "bar"]:
                    self.assertEqual(a[k], b[k])

        with self.subTest("cross-read"):
            hist_a = deltalake.DeltaTable(xdl_loc).history()
            hist_b = xdlake.DeltaTable(dt_loc).history()
            for a, b in zip(hist_a, hist_b):
                self.assertEqual(a["version"], b["version"])
                for k in ["foo", "bar"]:
                    self.assertEqual(a[k], b[k])

    def test_schema_change(self):
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        dt_loc = f"{self.scratch_folder}/{uuid4()}"

        tables = [self.gen_table() for _ in range(3)]
        table_new_schema = self.gen_table(additional_cols=["new_column"])

        for t in tables:
            xdl = xdl.write(t, mode="append")
            deltalake.write_deltalake(dt_loc, t, mode="append")

        with self.subTest("xdlake should raise"):
            with self.assertRaises(ValueError):
                xdl.write(table_new_schema, mode="append")

        with self.subTest("should work"):
            xdl = xdl.write(table_new_schema, mode="append", schema_mode="merge")
            deltalake.write_deltalake(dt_loc, table_new_schema, mode="append", schema_mode="merge", engine="rust")

        assert_arrow_table_equal(
            deltalake.DeltaTable(xdl.loc.path).to_pyarrow_table(),
            xdlake.DeltaTable(dt_loc).to_pyarrow_dataset().to_table(),
        )

    def test_delete(self):
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        dt_loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(23)]

        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=self.partition_by)
            deltalake.write_deltalake(dt_loc, at, partition_by=self.partition_by, mode="append")

        num_start_rows = xdl.to_pyarrow_table().to_pandas().shape[0]
        xdl = xdl.delete((((pc.field("float64") > pc.scalar(0.9)) | (pc.field("cats") == pc.scalar("A")))))
        deltalake.DeltaTable(dt_loc).delete("float64 > 0.9 or cats == 'A'")
        num_end_rows = xdl.to_pyarrow_table().to_pandas().shape[0]
        with self.subTest("should have actually deleted rows"):
            self.assertLess(num_end_rows, num_start_rows)

        with self.subTest("should aggree"):
            assert_arrow_table_equal(deltalake.DeltaTable(xdl.loc.path), xdlake.DeltaTable(dt_loc).to_pyarrow_table())

    def test_optimize(self):
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        dt_loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(23)]

        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=self.partition_by)
            deltalake.write_deltalake(dt_loc, at, partition_by=self.partition_by, mode="append")

        num_start_rows = xdl.to_pyarrow_table().to_pandas().shape[0]
        xdl = xdl.delete((((pc.field("float64") > pc.scalar(0.9)) | (pc.field("cats") == pc.scalar("A")))))
        deltalake.DeltaTable(dt_loc).delete("float64 > 0.9 or cats == 'A'")
        deltalake.DeltaTable(dt_loc).optimize.compact()

        with self.subTest("should have actually deleted rows"):
            self.assertLess(xdl.to_pyarrow_table().to_pandas().shape[0], num_start_rows)

        with self.subTest("should aggree"):
            assert_arrow_table_equal(deltalake.DeltaTable(xdl.loc.path), xdlake.DeltaTable(dt_loc).to_pyarrow_table())

    def test_merge(self):
        dt_loc = f"{self.scratch_folder}/{uuid4()}"
        for _ in range(5):
            deltalake.write_deltalake(dt_loc, self.gen_table(), partition_by=self.partition_by, mode="append")

        pre_merge_df = deltalake.DeltaTable(dt_loc).to_pandas().sort_values("order").reset_index(drop=True)
        merge_table = gen_merge_table(pre_merge_df.shape[0])
        deltalake.DeltaTable(dt_loc).merge(
            merge_table,
            "source.order = target.order",
            source_alias="source",
            target_alias="target",
        ).when_matched_update(
            {"float64": "source.bars"}
        ).execute()
        post_merge_df = deltalake.DeltaTable(dt_loc).to_pandas().sort_values("order").reset_index(drop=True)

        assert_frame_equal(pre_merge_df.drop(columns=["float64"]), post_merge_df.drop(columns=["float64"]))
        with self.assertRaises(AssertionError):
            assert_frame_equal(pre_merge_df, post_merge_df)

        with self.subTest("xdlake should read the merged table"):
            assert_arrow_table_equal(deltalake.DeltaTable(dt_loc), xdlake.DeltaTable(dt_loc))

    def test_restore(self):
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        dt_loc = f"{self.scratch_folder}/{uuid4()}"
        for _ in range(5):
            at = self.gen_table()
            xdl = xdl.write(at, partition_by=self.partition_by)
            deltalake.write_deltalake(dt_loc, at, partition_by=self.partition_by, mode="append")

        deltalake.DeltaTable(dt_loc).restore(2)
        xdl.restore(2)

        with self.subTest("should aggree"):
            assert_arrow_table_equal(deltalake.DeltaTable(xdl.loc.path), xdlake.DeltaTable(dt_loc).to_pyarrow_table())


def gen_merge_table(num_rows):
    data = dict()
    data["order"] = pa.array(range(num_rows))
    data["bars"] = pa.array(np.random.random(num_rows))
    return pa.Table.from_arrays(
        list(data.values()),
        names=list(data.keys()),
    )


if __name__ == '__main__':
    unittest.main()
