import unittest
from functools import lru_cache
from uuid import uuid4

import pyarrow.compute as pc

import xdlake

from tests.utils import TableGenMixin, assert_pandas_dataframe_equal


class TestCompatibilitySpark(TableGenMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.partition_by = list(self.table_gen.categoricals.keys())
        self.spark = self.create_spark_session()

    def gen_table(self, *args, **kwargs):
        t = super().gen_table(*args, **kwargs)
        # Spark doesn't support timezone-unaware timestamps
        return t.drop("timestamp_no_tz")

    @lru_cache
    def create_spark_session(self):
        import pyspark.sql
        import delta  # delta-spark

        builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
        return spark

    def test_read(self):
        import delta  # delta-spark

        sdt_loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(3)]
        for at in arrow_tables:
            # TODO: test timestamp as well -- spark is doing something wonky with dates?
            at = at.drop_columns(["timestamp"])
            spark_df = self.spark.createDataFrame(at.to_pandas())
            spark_df.write.format("delta").mode("append").save(sdt_loc, overwrite=True)
        spark_dt = delta.tables.DeltaTable.forPath(self.spark, sdt_loc)
        spark_dt.delete("float64 > 0.9")
        spark_dt.vacuum(0)
        spark_dt.optimize().executeCompaction()
        spark_dt = delta.tables.DeltaTable.forPath(self.spark, sdt_loc)
        assert_pandas_dataframe_equal(
            spark_dt.toDF().toPandas(),
            xdlake.DeltaTable(sdt_loc).to_pyarrow_table().to_pandas(),
        )

    def test_cross_read(self):
        import delta  # delta-spark

        known_incompatibilities = ["int8", "int16", "int32"]
        xdl_loc = f"{self.scratch_folder}/{uuid4()}"
        xdl = xdlake.DeltaTable(xdl_loc)
        sdt_loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(3)]
        for at in arrow_tables:
            # TODO: test timestamp as well -- spark is doing something wonky with dates?
            at = at.drop_columns(["timestamp"] + known_incompatibilities)
            xdl = xdl.write(at, partition_by=self.partition_by)
            spark_df = self.spark.createDataFrame(at.to_pandas())
            spark_df.write.format("delta").mode("append").save(sdt_loc, overwrite=True)
        spark_dt = delta.tables.DeltaTable.forPath(self.spark, sdt_loc)
        spark_dt.delete("float64 > 0.9")
        xdl = xdl.delete(pc.field("float64") > pc.scalar(0.9))
        spark_dt.vacuum(0)
        spark_dt.optimize().executeCompaction()
        assert_pandas_dataframe_equal(
            delta.tables.DeltaTable.forPath(self.spark, xdl_loc).toDF().toPandas(),
            xdlake.DeltaTable(sdt_loc).to_pyarrow_table().to_pandas(),
        )


if __name__ == '__main__':
    unittest.main()
