import os
import unittest
import warnings
from uuid import uuid4

import pyarrow as pa

from xdlake import dataset_utils, storage

from tests.utils import TableGenMixin, assert_arrow_table_equal


class TestDataset(TableGenMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        warnings.simplefilter("ignore", DeprecationWarning)
        self.tables = list()

    def gen_table(self, *args, **kwargs):
        arrow_table = super().gen_table(*args, **kwargs)
        self.tables.append(arrow_table)
        return arrow_table

    def test_dataset(self):
        table = self.gen_table()
        record_batches = self.gen_table().to_batches()
        ds = pa.dataset.dataset(self.gen_table())
        loc = os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
        pa.dataset.write_dataset(self.gen_table(), loc, format="parquet")
        paths = [os.path.join(root, f)
                 for root, _, files in os.walk(loc)
                 for f in files]
        dsr = dataset_utils.union_dataset([table, ds, *record_batches, *paths])
        self.assertIsInstance(dsr, pa.dataset.UnionDataset)
        assert_arrow_table_equal(pa.concat_tables(self.tables), dsr.to_table())

    def test_dataset_schemas_merge(self):
        record_batches = self.gen_table().to_batches()
        table = self.gen_table(additional_cols="foo")
        ds = pa.dataset.dataset(self.gen_table(additional_cols="bar"))
        loc = os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
        pa.dataset.write_dataset(self.gen_table(additional_cols="baz"), loc, format="parquet")
        paths = [os.path.join(root, f)
                 for root, _, files in os.walk(loc)
                 for f in files]
        dsr = dataset_utils.union_dataset([table, ds, *record_batches, *paths], schema_mode="merge")
        self.assertIsInstance(dsr, pa.dataset.UnionDataset)
        assert_arrow_table_equal(pa.concat_tables(self.tables, promote_options="default"), dsr.to_table())

    def test_dataset_schemas_common(self):
        table = self.gen_table()
        original_schema = table.schema
        record_batches = self.gen_table(additional_cols="foo").to_batches()
        ds = pa.dataset.dataset(self.gen_table(additional_cols="bar"))
        loc = os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
        pa.dataset.write_dataset(self.gen_table(additional_cols="baz"), loc, format="parquet")
        paths = [os.path.join(root, f)
                 for root, _, files in os.walk(loc)
                 for f in files]
        dsr = dataset_utils.union_dataset([table, ds, *record_batches, *paths])
        self.assertIsInstance(dsr, pa.dataset.UnionDataset)
        self.assertEqual(dsr.schema, original_schema)

    def test_paths_with_different_schema(self):
        new_col_name = f"{uuid4()}"[:6]
        loc = storage.Location.with_location(f"s3://test-xdlake/{uuid4()}")
        tables = {loc.append_path(f"{uuid4()}.parquet"): self.gen_table()
                  for _ in range(2)}
        tables[loc.append_path(f"{uuid4()}.parquet")] = self.gen_table(additional_cols=new_col_name)
        for filepath in tables:
            pa.parquet.write_table(tables[filepath], filepath.path, filesystem=loc.fs)

        paths = [sob for sob in storage.Location.with_location(loc).list_files()]

        with self.subTest("common schema"):
            ds = dataset_utils.union_dataset(paths)
            self.assertNotIn(new_col_name, ds.to_table().column_names)

        with self.subTest("merge schema"):
            ds = dataset_utils.union_dataset(paths, schema_mode="merge")
            self.assertIn(new_col_name, ds.to_table().column_names)


if __name__ == '__main__':
    unittest.main()
