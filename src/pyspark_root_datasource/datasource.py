import math
import warnings
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

import awkward as ak
import pyarrow as pa
import uproot

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.pandas.types import from_arrow_schema
from pyspark.sql.types import StructType

from ._utils import (
    _parse_bool, _parse_int, expand_paths,
    cast_unsigned_to_signed, select_arrow_columns,
)

@dataclass
class ROOTPartition(InputPartition):
    """One partition = (one file, a range of entry indices)."""
    file_path: str
    tree: str
    entry_start: int
    entry_stop: int
    index: int  # sequential id


class UprootDataSource(DataSource):
    """
    Spark 4 Python DataSource for reading ROOT files via uproot/awkward/pyarrow.

    DataSource short name:
      root

    Required option:
      - path: one path, comma-separated list, directory, or local glob (e.g. "/data/*.root").
              Remote URLs (e.g. root://xrootd.site//path/file.root) are OK (no remote listing).

    Common options:
      - tree            : TTree name (default: "Events")
      - step_size       : rows per partition (default: 1_000_000)
      - num_partitions  : override partitions per file (takes precedence over step_size)
      - entry_start     : per-file entry start (default: 0)
      - entry_stop      : per-file entry stop (default: all)
      - list_to32       : cast variable-length lists to 32-bit offsets for Arrow (default: true)
      - extensionarray  : use extension types in Arrow conversion (default: false)
      - cast_unsigned   : cast uint* to signed types (default: true)
      - columns         : comma-separated branch names (optional; otherwise use Spark schema fields)
      - recursive       : expand directories recursively (default: false)
      - ext             : pattern for directory expansion (default: "*.root")

    Additional controls:
      - sample_rows        : rows used for schema inference (default: 1000)
      - inner_step_size    : when >0, stream a partition in smaller uproot chunks (default: auto)
      - arrow_max_chunksize: when >0, limit Arrow RecordBatch row count per yielded batch (default: 0 = PyArrow default)
    """

    DEFAULT_TREE = "Events"

    def __init__(self, options):
        super().__init__(options)
        opts = dict(options or {})

        # Path expansion
        self.recursive = _parse_bool(opts.get("recursive"), False)
        self.ext = opts.get("ext", "*.root")
        self.paths: List[str] = expand_paths(
            opts.get("path") or opts.get("paths"),
            recursive=self.recursive,
            dir_ext=self.ext,
        )
        if not self.paths:
            original = (opts.get("path") or opts.get("paths") or "").strip()
            tips = ' Try options like recursive=true or ext="*.root" if you passed a directory.'
            raise Exception(f"You must specify a 'path' that resolves to files. Got: '{original}'.{tips}")

        # root:// dependency checks
        if any(p.startswith("root://") for p in self.paths):
            try:
                import fsspec  # noqa: F401
            except Exception as e:
                raise Exception("Reading root:// URLs requires 'fsspec'. Install with: pip install fsspec") from e
            try:
                import fsspec_xrootd  # noqa: F401
            except Exception as e:
                raise Exception("Reading root:// URLs requires 'fsspec-xrootd'. Install with: pip install fsspec-xrootd") from e
            try:
                import XRootD  # noqa: F401
            except Exception as e:
                raise Exception(
                    "The XRootD client is required. On Linux, a reliable route is:\n"
                    "  conda install -c conda-forge xrootd\n"
                    "or system packages; then `pip install XRootD`."
                ) from e

        # Core options
        self.tree_name: str = opts.get("tree", self.DEFAULT_TREE)
        self.step_size: int = _parse_int(opts.get("step_size"), 1_000_000) or 1_000_000
        self.num_partitions_opt: Optional[int] = _parse_int(opts.get("num_partitions"), None)
        self.entry_start: int = _parse_int(opts.get("entry_start"), 0) or 0
        self.entry_stop_opt: Optional[int] = _parse_int(opts.get("entry_stop"), None)

        # Conversion flags
        self.list_to32: bool = _parse_bool(opts.get("list_to32"), True)
        self.extensionarray: bool = _parse_bool(opts.get("extensionarray"), False)
        self.cast_unsigned: bool = _parse_bool(opts.get("cast_unsigned"), True)

        # Inference controls
        self.sample_rows: int = _parse_int(opts.get("sample_rows"), 1000) or 1000

        # Chunking controls (auto default to streaming to reduce memory pressure)
        self.inner_step_size: int = _parse_int(opts.get("inner_step_size"), 0) or 0
        self.arrow_max_chunksize: int = _parse_int(opts.get("arrow_max_chunksize"), 0) or 0

        # Column selection from option (trim whitespace)
        cols = opts.get("columns")
        self.option_columns: Optional[List[str]] = (
            [c.strip() for c in cols.split(",")] if cols else None
        )

    @classmethod
    def name(cls):
        return "root"

    # --- Schema --------------------------------------------------------------

    def _open_tree(self, path: str):
        try:
            f = uproot.open(path)
        except Exception as e:
            raise Exception(f"Failed to open file: {path}. {e}") from e
        try:
            t = f[self.tree_name]
        except Exception:
            try:
                keys = list(f.keys())
            except Exception:
                keys = []
            f.close()
            show = ", ".join(keys[:20]) + (" ..." if len(keys) > 20 else "")
            raise Exception(f"TTree '{self.tree_name}' not found in {path}. Available keys: {show}")
        return f, t

    def _sample_arrow_schema(self) -> pa.Schema:
        path0 = self.paths[0]
        f, t = self._open_tree(path0)
        try:
            n = getattr(t, "num_entries", None)
            if n is None:
                n = len(t)
            # pick at least 1 row if available; Arrow schema is defined with 0 rows but some files behave better with 1
            stop = min(max(self.sample_rows, 1), n)
            expressions = self.option_columns or None
            batch = t.arrays(expressions=expressions, entry_start=0, entry_stop=stop, library="ak")
            tbl = ak.to_arrow_table(batch, extensionarray=self.extensionarray, list_to32=self.list_to32)
            if self.cast_unsigned:
                tbl = cast_unsigned_to_signed(tbl)
            return tbl.schema
        finally:
            f.close()

    def schema(self):
        """
        Spark 4 Python DataSource requires schema() to return a StructType (not None).
        We infer from a small sample of the first file. Users can still prune or override
        via DataFrameReader.schema(...) or .option("columns", ...).
        """
        try:
            arrow_schema = self._sample_arrow_schema()
            return from_arrow_schema(arrow_schema)
        except Exception as e:
            raise Exception(
                f"Schema inference failed: {e}. "
                "Check that 'path' and 'tree' are correct and files are readable."
            )

    # --- Reader --------------------------------------------------------------

    def reader(self, schema: StructType):
        return UprootDataSourceReader(schema, self.options, self.paths, self.tree_name,
                                      self.step_size, self.num_partitions_opt,
                                      self.entry_start, self.entry_stop_opt,
                                      self.list_to32, self.extensionarray,
                                      self.cast_unsigned, self.option_columns,
                                      self.inner_step_size, self.arrow_max_chunksize)


class UprootDataSourceReader(DataSourceReader):
    def __init__(
        self,
        schema: Optional[StructType],
        options,
        paths: List[str],
        tree_name: str,
        step_size: int,
        num_partitions_opt: Optional[int],
        entry_start: int,
        entry_stop_opt: Optional[int],
        list_to32: bool,
        extensionarray: bool,
        cast_unsigned: bool,
        option_columns: Optional[List[str]],
        inner_step_size: int,
        arrow_max_chunksize: int,
    ):
        self.schema = schema
        self.options = options
        self.paths = paths
        self.tree_name = tree_name
        self.step_size = step_size
        self.num_partitions_opt = num_partitions_opt
        self.entry_start = entry_start
        self.entry_stop_opt = entry_stop_opt
        self.list_to32 = list_to32
        self.extensionarray = extensionarray
        self.cast_unsigned = cast_unsigned
        self.option_columns = option_columns
        self.arrow_max_chunksize = arrow_max_chunksize

        # Columns to read: prefer Spark-provided schema field names, otherwise "columns" option
        if self.schema and len(self.schema.fields) > 0:
            self.columns = [f.name for f in self.schema.fields]
            if option_columns:
                warnings.warn("Ignoring .option('columns', ...) because a Spark schema was provided.")
        else:
            self.columns = self.option_columns

        # Default to streaming if not specified to reduce memory usage on large files
        self.inner_step_size = inner_step_size if inner_step_size and inner_step_size > 0 else min(self.step_size, 200_000)

        # Compute partitions lazily at first call
        self._parts: Optional[List[ROOTPartition]] = None

    # ---- Partitioning -------------------------------------------------------

    def _file_num_entries(self, path: str) -> int:
        f, t = None, None
        try:
            f = uproot.open(path)
            t = f[self.tree_name]
            n = getattr(t, "num_entries", None)
            return int(n if n is not None else len(t))
        finally:
            try:
                if f is not None:
                    f.close()
            except Exception:
                pass

    def _compute_partitions(self) -> List[ROOTPartition]:
        parts: List[ROOTPartition] = []
        idx = 0
        for p in self.paths:
            n = self._file_num_entries(p)
            gstart = self.entry_start
            gstop = self.entry_stop_opt if self.entry_stop_opt is not None else n
            gstop = min(gstop, n)
            if gstart >= gstop:
                continue
            if self.num_partitions_opt:
                span = gstop - gstart
                step = max(1, math.ceil(span / self.num_partitions_opt))
            else:
                step = max(1, self.step_size)
            start = gstart
            while start < gstop:
                stop = min(start + step, gstop)
                parts.append(ROOTPartition(file_path=p, tree=self.tree_name,
                                           entry_start=start, entry_stop=stop,
                                           index=idx))
                idx += 1
                start = stop
        return parts

    def partitions(self) -> Sequence[ROOTPartition]:
        if self._parts is None:
            self._parts = self._compute_partitions()
        return self._parts

    # ---- Reading ------------------------------------------------------------

    def _yield_batches_from_table(self, tbl: pa.Table):
        if self.arrow_max_chunksize and self.arrow_max_chunksize > 0:
            for rb in tbl.to_batches(max_chunksize=self.arrow_max_chunksize):
                yield rb
        else:
            for rb in tbl.to_batches():
                yield rb

    def _to_table(self, batch_ak) -> pa.Table:
        tbl = ak.to_arrow_table(
            batch_ak,
            extensionarray=self.extensionarray,
            list_to32=self.list_to32
        )
        if self.cast_unsigned:
            tbl = cast_unsigned_to_signed(tbl)
        # Spark schema or option-driven projection
        tbl = select_arrow_columns(tbl, self.columns)
        return tbl

    def _read_one_partition_streamed(self, part: ROOTPartition) -> Iterable[pa.RecordBatch]:
        with uproot.open(part.file_path) as f:
            try:
                t = f[part.tree]
            except Exception:
                keys = []
                try:
                    keys = list(f.keys())
                except Exception:
                    pass
                raise Exception(f"TTree '{part.tree}' not found in {part.file_path}. Keys: {keys[:20]}")
            for batch in t.iterate(
                expressions=self.columns or None,
                entry_start=part.entry_start,
                entry_stop=part.entry_stop,
                step_size=self.inner_step_size,
                library="ak",
            ):
                tbl = self._to_table(batch)
                yield from self._yield_batches_from_table(tbl)

    def _read_one_partition_whole(self, part: ROOTPartition) -> Iterable[pa.RecordBatch]:
        with uproot.open(part.file_path) as f:
            try:
                t = f[part.tree]
            except Exception:
                keys = []
                try:
                    keys = list(f.keys())
                except Exception:
                    pass
                raise Exception(f"TTree '{part.tree}' not found in {part.file_path}. Keys: {keys[:20]}")
            batch = t.arrays(
                expressions=self.columns or None,
                entry_start=part.entry_start,
                entry_stop=part.entry_stop,
                library="ak",
            )
            tbl = self._to_table(batch)
            yield from self._yield_batches_from_table(tbl)

    def read(self, partition: ROOTPartition):
        # With the auto default, this will typically stream
        if self.inner_step_size and self.inner_step_size > 0:
            yield from self._read_one_partition_streamed(partition)
        else:
            yield from self._read_one_partition_whole(partition)
