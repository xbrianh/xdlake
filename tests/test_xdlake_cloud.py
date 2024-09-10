import os
import unittest
from uuid import uuid4

import pyarrow as pa
import pyarrow.dataset

import xdlake

from tests.base_xdlake_test import BaseXdlakeTest
from tests.utils import assert_arrow_table_equal, AzureSucksCredential


class TestXdLakeCloud(BaseXdlakeTest):
    def test_remote_log(self):
        arrow_tables = [self.gen_table() for _ in range(3)]
        expected = pa.concat_tables(arrow_tables)
        tests = [
            (f"s3://test-xdlake/tests/{uuid4()}", f"{self.scratch_folder}/{uuid4()}"),
            (f"{self.scratch_folder}/{uuid4()}", f"s3://test-xdlake/tests/{uuid4()}"),
        ]
        for data_loc, log_loc in tests:
            xdl = xdlake.DeltaTable(data_loc, log_loc)
            with self.subTest(data_loc=data_loc, log_loc=log_loc):
                for at in arrow_tables:
                    xdl = xdl.write(at)
                assert_arrow_table_equal(expected, xdl.to_pyarrow_table())
                self._test_clone(xdl)
                self._test_delete(xdl)

    def test_s3(self):
        partition_by = self.partition_by[:1]
        arrow_tables = [self.gen_table() for _ in range(3)]
        xdl = xdlake.DeltaTable(f"s3://test-xdlake/tests/{uuid4()}")
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=partition_by)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        self._test_clone(xdl)
        self._test_delete(xdl)

    def test_gs(self):
        partition_by = self.partition_by[:1]
        arrow_tables = [self.gen_table() for _ in range(3)]
        xdl = xdlake.DeltaTable(f"gs://test-xdlake/tests/{uuid4()}")
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=partition_by)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        self._test_clone(xdl)
        self._test_delete(xdl)

    def test_azure_storage(self):
        partition_by = self.partition_by[:1]
        arrow_tables = [self.gen_table() for _ in range(3)]

        storage_options = {
            "account_name": "xdlake",
            "credential": AzureSucksCredential(),
        }
        xdl = xdlake.DeltaTable(f"az://test-xdlake/tests/{uuid4()}", storage_options=storage_options)
        for at in arrow_tables:
            xdl = xdl.write(at, partition_by=partition_by)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        xdlake.storage.register_default_filesystem_for_protocol("az", storage_options=storage_options)
        self._test_clone(xdl)
        self._test_delete(xdl)

    def test_import_refs(self):
        paths = [os.path.join(f"{self.scratch_folder}", f"{uuid4()}", f"{uuid4()}.parquet") for _ in range(2)]
        paths += [f"s3://test-xdlake/{uuid4()}.parquet" for _ in range(2)]
        paths += [f"gs://test-xdlake/{uuid4()}.parquet" for _ in range(2)]
        paths += [f"az://test-xdlake/{uuid4()}.parquet" for _ in range(2)]
        storage_options = {
            "account_name": "xdlake",
            "credential": AzureSucksCredential(),
        }
        xdlake.storage.register_default_filesystem_for_protocol("az", storage_options=storage_options)
        arrow_tables, written_files = self.gen_parquets(locations=paths)
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}").import_refs(written_files)
        assert_arrow_table_equal(pa.concat_tables(arrow_tables), xdl.to_pyarrow_table())
        self._test_clone(xdl)
        self._test_delete(xdl)

    def test_import_refs_with_partitions(self):
        storage_options = {
            "account_name": "xdlake",
            "credential": AzureSucksCredential(),
        }
        xdlake.storage.register_default_filesystem_for_protocol("az", storage_options=storage_options)
        hive_partition_schema = pa.unify_schemas([self.table_gen.categorical_schema, pa.schema([("bool_", pa.bool_())])])
        partitionings = {
            "hive": pyarrow.dataset.partitioning(flavor="hive", schema=hive_partition_schema),
            "filename": pyarrow.dataset.partitioning(flavor="filename", schema=self.table_gen.categorical_schema),
            None: pyarrow.dataset.partitioning(flavor=None, schema=self.table_gen.categorical_schema),
        }

        datasets = list()
        arrow_tables = list()
        for flavor, partitioning in partitionings.items():
            foreign_refs_loc = f"az://test-xdlake/{uuid4()}"  # os.path.join(f"{self.scratch_folder}", f"{uuid4()}")
            new_tables, written_files = self.gen_parquets(
                locations=[foreign_refs_loc],
                partitioning=partitioning,
            )
            arrow_tables.extend(new_tables)
            ds = pyarrow.dataset.dataset(
                written_files,
                partitioning=partitioning,
                partition_base_dir=foreign_refs_loc,
                filesystem=xdlake.storage.get_filesystem(foreign_refs_loc),
            )
            datasets.append(ds)

        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}").import_refs(datasets, partition_by=self.partition_by)

        assert_arrow_table_equal(
            pa.concat_tables(arrow_tables),
            xdl.to_pyarrow_table()
        )
        self._test_clone(xdl)
        self._test_delete(xdl)


if __name__ == '__main__':
    unittest.main()
