import random
from contextlib import contextmanager
from tempfile import TemporaryDirectory

import numpy as np
import pyarrow as pa
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

    @contextmanager
    def with_extra_columns(self, cols: str | list[str]):
        if isinstance(cols, str):
            cols = [cols]
        old_cols = self.columns.copy()
        try:
            self.columns.extend(cols)
            yield self
        finally:
            self.columns = old_cols

def assert_arrow_table_equal(a, b):
    a = a.to_pandas().sort_values("order").sort_index(axis=1).reset_index(drop=True)
    b = b.to_pandas().sort_values("order").sort_index(axis=1).reset_index(drop=True)
    assert_frame_equal(a, b)


class TableGenMixin:
    def setUp(self):
        self.td = TemporaryDirectory()
        self.scratch_folder = self.td.name
        self.table_gen = TableGen()
        self.tables = list()

    def tearDown(self):
        self.td.cleanup()

    def gen_table(self, *, additional_cols: list | str | None = None):
        additional_cols = additional_cols or []
        if isinstance(additional_cols, str):
            additional_cols = [additional_cols]
        with self.table_gen.with_extra_columns(additional_cols):
            t = next(self.table_gen)
        self.tables.append(t)
        return t

    def gen_tables(self, num_tables: int, *, additional_cols: list | str | None = None):
        return [self.gen_table(additional_cols=additional_cols)
                for _ in range(num_tables)]

    def gen_parquets_for_table(self, table_loc, **kwargs) -> dict:
        if table_loc.startswith("s3://"):
            fs = xdlake.storage.get_filesystem("s3")
        else:
            fs = xdlake.storage.get_filesystem("file")

        t = self.gen_table()

        written_files = list()

        def visitor(visited_file):
            written_files.append(visited_file.path)

        pa.dataset.write_dataset(
            t,
            table_loc,
            format="parquet",
            filesystem=fs,
            file_visitor=visitor,
            **kwargs
        )

        return {"table": t, "written_files": written_files}

    def gen_parquets_for_tables(self, *, table_locs, **kwargs):
        tables, paths = list(), list()
        for loc in table_locs:
            info = self.gen_parquets_for_table(loc, **kwargs)
            tables.append(info["table"])
            paths.extend(info["written_files"])
        return tables, paths
