import os
import glob
import warnings
from typing import List, Optional

import pyarrow as pa
import pyarrow.compute as pc


def _parse_bool(x: Optional[str], default: bool) -> bool:
    if x is None:
        return default
    x = str(x).strip().lower()
    if x in ("1", "true", "yes", "y", "on"):
        return True
    if x in ("0", "false", "no", "n", "off"):
        return False
    return default


def _parse_int(x: Optional[str], default: Optional[int]) -> Optional[int]:
    if x is None:
        return default
    try:
        return int(x)
    except Exception:
        return default


def _normalize_local_path(p: str) -> str:
    # Expand ~ and make absolute for local FS convenience
    return os.path.abspath(os.path.expanduser(p))


def expand_paths(
    path_option: Optional[str],
    *,
    recursive: bool = False,
    dir_ext: str = "*.root",
) -> List[str]:
    """
    Accepts:
      - a single file path or URL (local, xrootd, etc.)
      - a comma-separated list of paths
      - a directory (local FS)
      - a glob pattern (local FS; '**' honored when recursive=True)
    Returns an expanded, de-duplicated list (order preserved).
    Remote URLs are left as-is (no remote listing/expansion).
    """
    if not path_option:
        return []

    # normalize extension pattern (accept ".root" too)
    ext = dir_ext
    if ext and not any(ch in ext for ch in ["*", "?", "["]):
        if not ext.startswith("."):
            ext = f".{ext}"
        ext = f"*{ext}"

    parts = [p.strip() for p in path_option.split(",") if p.strip()]
    out: List[str] = []
    seen = set()

    for raw in parts:
        p = raw
        # leave non-local schemes untouched
        if "://" in p and not p.startswith("file://"):
            if p not in seen:
                out.append(p)
                seen.add(p)
            continue

        p_local = _normalize_local_path(p)

        # Local directory → append pattern
        if os.path.isdir(p_local):
            pattern = os.path.join(p_local, "**", ext) if recursive else os.path.join(p_local, ext)
            hits = sorted(glob.glob(pattern, recursive=recursive))
            for h in hits:
                if h not in seen:
                    out.append(h)
                    seen.add(h)
            continue

        # Local glob/wildcards
        if any(c in p_local for c in ["*", "?", "["]):
            # if the user didn't mean a glob but the path contains special chars, escape would be needed,
            # however we keep the user's intent here.
            hits = sorted(glob.glob(p_local, recursive=recursive))
            for h in hits:
                if h not in seen:
                    out.append(h)
                    seen.add(h)
            continue

        # Plain local path
        if p_local not in seen:
            out.append(p_local)
            seen.add(p_local)

    return out


def _fix_type_unsigned_to_signed(t: pa.DataType) -> pa.DataType:
    """Return a type with any uint* replaced by matching signed types; preserve fixed-size-list sizes."""
    if pa.types.is_uint8(t):   return pa.int8()
    if pa.types.is_uint16(t):  return pa.int16()
    if pa.types.is_uint32(t):  return pa.int32()
    if pa.types.is_uint64(t):  return pa.int64()

    if pa.types.is_list(t):
        return pa.list_(_fix_type_unsigned_to_signed(t.value_type))
    if pa.types.is_large_list(t):
        return pa.large_list(_fix_type_unsigned_to_signed(t.value_type))
    if pa.types.is_fixed_size_list(t):
        # IMPORTANT: older/newer PyArrow builds don’t expose pa.fixed_size_list(...)
        # The portable way is pa.list_(..., list_size=N) which yields FixedSizeListType.
        return pa.list_(_fix_type_unsigned_to_signed(t.value_type), list_size=t.list_size)

    if pa.types.is_struct(t):
        return pa.struct([(f.name, _fix_type_unsigned_to_signed(f.type)) for f in t])

    if pa.types.is_map(t):
        return pa.map_(
            _fix_type_unsigned_to_signed(t.key_type),
            _fix_type_unsigned_to_signed(t.item_type),
        )

    return t


def cast_unsigned_to_signed(table: pa.Table) -> pa.Table:
    """
    Spark doesn't support unsigned ints. Cast any uint* to the corresponding signed type.
    Also fix nested list/struct/map types while PRESERVING fixed-size lists.

    Strategy:
      - Compute target schema via _fix_type_unsigned_to_signed
      - Attempt vectorized cast with pyarrow.compute.cast per column (ChunkedArray)
      - If casting fails for a column, fall back to original column (last-resort safety)
    """
    tgt_fields = []
    new_cols = []
    for i, col in enumerate(table.itercolumns()):
        src_type = col.type
        tgt_type = _fix_type_unsigned_to_signed(src_type)
        tgt_fields.append((table.schema.names[i], tgt_type))
        if tgt_type == src_type:
            new_cols.append(col)
            continue
        try:
            new_cols.append(pc.cast(col, tgt_type))
        except Exception:
            # Fallback if complex nested cast fails: keep original to avoid crashing;
            # downstream may still handle (or user can disable cast via option).
            warnings.warn(f"Unsigned→signed cast fallback (kept original) for column '{table.schema.names[i]}'")
            new_cols.append(col)

    new_schema = pa.schema(tgt_fields)
    return pa.Table.from_arrays(new_cols, schema=new_schema)

def select_arrow_columns(table: pa.Table, columns: Optional[List[str]]) -> pa.Table:
    """
    Project to given top-level columns.
    Note: nested (dotted) selections are not supported yet; warn and keep top-level fallback.
    """
    if not columns:
        return table

    if any("." in c for c in columns):
        warnings.warn("Nested column pruning is not yet supported; applying top-level projection only.")

    keep = [c for c in columns if c in table.column_names]
    missing = [c for c in columns if c not in table.column_names]
    if missing:
        warnings.warn(f"Ignoring missing columns: {missing}")

    return table.select(keep) if keep else table
