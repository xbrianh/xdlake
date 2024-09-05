import unittest
from uuid import uuid4

import deltalake
import pyarrow.compute as pc
import pyspark.sql
import delta  # delta-spark

import xdlake

from tests.utils import TableGenMixin, assert_arrow_table_equal


def create_spark_session():
    builder = pyspark.sql.SparkSession.builder.appName("xdlake_tests") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


spark = create_spark_session()


class TestCompatibility(TableGenMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.partition_by = list(self.table_gen.categoricals.keys())

    def test_doom(self):
        xdl = xdlake.DeltaTable(f"{self.scratch_folder}/{uuid4()}")
        sdt_loc = f"{self.scratch_folder}/{uuid4()}"
        arrow_tables = [self.gen_table() for _ in range(3)]
        for at in arrow_tables:
            spark_df = spark.createDataFrame(at.to_pandas())
            spark_df.write.format("delta").mode("append").save(sdt_loc, overwrite=True)
        sdt = delta.tables.DeltaTable.forPath(spark, sdt_loc)
        df = sdt.toDF().toPandas()
        print(df)
        df = xdlake.DeltaTable(sdt_loc).to_pyarrow_table().to_pandas()
        print(df)


if __name__ == '__main__':
    unittest.main()
