import os
import pytz
import random
import datetime
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from tempfile import TemporaryDirectory

import numpy as np
import pyarrow as pa
from pandas.testing import assert_frame_equal
from azure.identity import DefaultAzureCredential

import xdlake


def random_datetimes(N, tz=pytz.UTC):
    seed = datetime.datetime.now(tz=tz)
    return [seed + datetime.timedelta(days=random.randint(0, 10000)) for _ in range(N)]

deltatable_types = {
        pa.bool_.__name__: (pa.bool_, lambda N: np.random.choice([True, False], size=N)),
        pa.int8.__name__: (pa.int8, lambda N: np.random.randint(-2 ** 8 / 2, 2 ** 8 / 2 - 1, size=N).astype(np.int8)),
        pa.int16.__name__: (pa.int16, lambda N: np.random.randint(-2 ** 16 / 2, 2 ** 16 / 2 - 1, size=N).astype(np.int16)),
        pa.int32.__name__: (pa.int32, lambda N: np.random.randint(-2 ** 32 / 2, 2 ** 32 / 2 - 1, size=N).astype(np.int32)),
        pa.int64.__name__: (pa.int64, lambda N: np.random.randint(-2 ** 64 / 2, 2 ** 64 / 2 - 1, size=N, dtype=np.int64)),

        # maybe don't support these
        # pa.uint8.__name__: (pa.uint8, lambda N: np.random.randint(0, 255, size=N).astype(np.uint8)),
        # pa.uint16.__name__:(pa.uint16, lambda N: np.random.randint(0, 2 ** 16, size=N).astype(np.uint16)),
        # pa.uint32.__name__:(pa.uint32, lambda N: np.random.randint(0, 2 ** 32, size=N).astype(np.uint32)),
        # pa.uint64.__name__:(pa.uint64, lambda N: np.random.randint(0, 2 ** 63, size=N).astype(np.uint64)),

        pa.date32.__name__: (pa.date32, lambda N: random_datetimes(N)),
        "timestamp_no_tz": (pa.timestamp, lambda N: random_datetimes(N, tz=None)),
        pa.timestamp.__name__: (pa.timestamp, lambda N: random_datetimes(N)),

        # pa.date64.__name__:(pa.date64, lambda N: np.datetime64("1805-01-01") + np.random.randint(0, 100, size=N)),
        # deltalake chokes on this for some reason

        # hould support float32?
        # pa.float32.__name__: (pa.float32, lambda N: np.random.rand(N).astype(np.float32)),

        pa.float64.__name__: (pa.float64, lambda N: np.random.rand(N).astype(np.float64)),
        pa.binary.__name__: (pa.binary, lambda N: [os.urandom(10) for _ in range(N)]),
        pa.string.__name__: (pa.string, lambda N: np.random.choice(["foo", "bar", "baz"], size=N)),
        pa.utf8.__name__:(pa.utf8, lambda N: np.random.choice(["foo", "bar", "baz"], size=N)),

        # support these? something about large type bs in deltalake
        # pa.large_utf8.__name__: (pa.large_utf8, lambda N: np.random.choice(["foo", "bar", "baz"], size=N)),
        # pa.large_binary.__name__: (pa.large_binary, lambda N: np.random.bytes(N)),
        # pa.large_string.__name__: (pa.large_string, lambda N: np.random.choice(["foo", "bar", "baz"], size=N)),
}

class TableGen:
    def __init__(self):
        self.columns = list(deltatable_types)
        self.categoricals = {
            "cats": ["S", "A", "D"],
            "bats": [1, 2, 3],
        }
        self.categorical_schema = pa.schema([("cats", pa.string()), ("bats", pa.int64())])
        self.order_parm = 0

    def __next__(self) -> pa.Table:
        data = list()
        for c in self.columns:
            if c in deltatable_types:
                arrow_type, gen = deltatable_types[c]
                values = gen(11)
                if arrow_type == pa.timestamp:
                    at = pa.timestamp("us", tz=values[0].tzinfo)
                else:
                    at = arrow_type()
                data.append(pa.array(values, type=at))
            else:
                data.append(pa.array(np.random.random(11), type=pa.float64()))
        t = pa.Table.from_arrays(data, names=self.columns)

        order = [float(i) + self.order_parm for i in range(len(t))]
        self.order_parm += len(t)

        for name, choices in self.categoricals.items():
            d = [random.choice(choices) for _ in range(len(t))]
            t = t.append_column(name, [d])
        t = t.append_column("order", pa.array(order, pa.int64()))
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


def assert_pandas_dataframe_equal(a, b):
    a = a.sort_values("order").sort_index(axis=1).reset_index(drop=True)
    b = b.sort_values("order").sort_index(axis=1).reset_index(drop=True)
    assert_frame_equal(a, b)


def assert_arrow_table_equal(a, b):
    assert_pandas_dataframe_equal(a.to_pandas(), b.to_pandas())


class TableGenMixin:
    def setUp(self):
        self.td = TemporaryDirectory()
        self.scratch_folder = self.td.name
        self.table_gen = TableGen()

    def tearDown(self):
        self.td.cleanup()

    def gen_table(self, *, additional_cols: list | str | None = None):
        additional_cols = additional_cols or []
        if isinstance(additional_cols, str):
            additional_cols = [additional_cols]
        with self.table_gen.with_extra_columns(additional_cols):
            t = next(self.table_gen)
        return t

    def gen_tables(self, num_tables: int, *, additional_cols: list | str | None = None):
        return [self.gen_table(additional_cols=additional_cols)
                for _ in range(num_tables)]

    def _gen_parquets(self, location, **kwargs) -> tuple[pa.Table, list[str]]:
        fs = xdlake.storage.get_filesystem(location)

        t = self.gen_table()

        written_files = list()

        def visitor(visited_file):
            written_files.append(visited_file.path)

        pa.dataset.write_dataset(
            t,
            location,
            format="parquet",
            filesystem=fs,
            file_visitor=visitor,
            **kwargs
        )

        return t, written_files

    def gen_parquets(self, *, locations, **kwargs) -> tuple[list[pa.Table], list[str]]:
        tables, paths = list(), list()
        with ThreadPoolExecutor() as e:
            for f in [e.submit(self._gen_parquets, loc, **kwargs) for loc in locations]:
                t, files = f.result()
                tables.append(t)
                paths.extend(files)
        return tables, paths


class AzureSucksCredential(DefaultAzureCredential):
    async def close(self, *args, **kwargs):
        # thanks but I don't need piles of irrelevant exceptions
        pass
