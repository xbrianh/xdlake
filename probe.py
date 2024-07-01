import shutil

import deltalake
from tests.utils import TableGen

table_gen = TableGen()

shutil.rmtree("tdl", ignore_errors=True)
deltalake.write_deltalake("tdl", next(table_gen), partition_by=["cats"])
deltalake.write_deltalake("tdl", next(table_gen), mode="append")
