"""
Utilities for normalizing heterogeneous execution results into JSON-safe rows.
"""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import cast


_DEFAULT_TABULAR_SOURCE_PRECEDENCE = (
    "payload",
    "analysis_result",
    "execution_result",
)
_TABULAR_CONTAINER_KEYS = (
    "rows",
    "data",
    "result",
    "results",
    "records",
    "items",
    "table",
    "value",
)
_TABULAR_COLUMNS_KEYS = ("columns", "column_names", "headers", "fields")


class ResultNormalizerError(Exception):
    """Raised when a result payload cannot be normalized into row objects."""


def normalize_result(data: object) -> list[dict[str, object]]:
    """Normalize supported result shapes into list[dict]."""
    if data is None:
        return []

    if isinstance(data, Mapping):
        return [_normalize_mapping(data)]

    # Accept pandas-like objects without importing pandas to avoid type-stub warnings
    to_dict_fn = getattr(data, "to_dict", None)
    if callable(to_dict_fn):
        records = None
        try:
            # DataFrame-like: to_dict(orient="records") -> list[dict]
            records = to_dict_fn(orient="records")
        except TypeError:
            try:
                # Series-like: to_dict() -> mapping
                records = to_dict_fn()
            except Exception:
                records = None

        if isinstance(records, list) and records and isinstance(records[0], Mapping):
            return [_normalize_mapping(row) for row in records]
        if isinstance(records, Mapping):
            return [_normalize_mapping(records)]

    # handle pandas Series (single row-like mapping)
    # pandas Series case handled by generic to_dict logic above

    if isinstance(data, list):
        if not data:
            return []

        if all(isinstance(item, Mapping) for item in data):
            # items are already Mapping (checked above) so cast is unnecessary
            return [_normalize_mapping(item) for item in data]

        # support list-of-lists where first row is header or list-of-tuples
        if all(isinstance(item, (list, tuple)) for item in data):
            rows = [list(r) for r in cast(list[list[object]], data)]
            if not rows:
                return []
            header = rows[0]
            # if header looks like column names (strings or convertible), use it; otherwise
            # treat each inner sequence as a full-row with numeric column indices
            header_is_strings = all(isinstance(h, (str, bytes)) for h in header)
            normalized_rows: list[dict[str, object]] = []
            if header_is_strings:
                for row in rows[1:]:
                    normalized_rows.append(
                        {
                            str(column): row[index] if index < len(row) else None
                            for index, column in enumerate(header)
                        }
                    )
            else:
                # no header; use numeric column names from 0..n
                for row in rows:
                    normalized_rows.append(
                        {str(index): value for index, value in enumerate(row)}
                    )
            return normalized_rows

    raise ResultNormalizerError(
        f"Unsupported result type for normalization: {type(data).__name__}"
    )


def ensure_json_serializable(data: list[dict[str, object]]) -> list[dict[str, object]]:
    """Return a new JSON-safe copy of normalized rows."""
    serialized_rows: list[dict[str, object]] = []
    for row in data:
        serialized_rows.append(
            {
                str(key): _serialize_value(value)
                for key, value in row.items()
            }
        )
    return serialized_rows


def normalize_and_serialize(data: object) -> list[dict[str, object]]:
    """Normalize result shapes and coerce nested values into JSON-safe primitives."""
    return ensure_json_serializable(normalize_result(data))


def normalize_canonical_tabular_result(
    *,
    payload: object = None,
    analysis_result: object = None,
    execution_result: object = None,
    source_precedence: Sequence[str] = _DEFAULT_TABULAR_SOURCE_PRECEDENCE,
) -> list[dict[str, object]]:
    """Select and normalize the first supported tabular payload by source precedence."""
    selected = select_canonical_tabular_payload(
        payload=payload,
        analysis_result=analysis_result,
        execution_result=execution_result,
        source_precedence=source_precedence,
    )
    return ensure_json_serializable(normalize_result(selected))


def select_canonical_tabular_payload(
    *,
    payload: object = None,
    analysis_result: object = None,
    execution_result: object = None,
    source_precedence: Sequence[str] = _DEFAULT_TABULAR_SOURCE_PRECEDENCE,
) -> object:
    """Return the first candidate payload that can be normalized as tabular rows."""
    candidates = {
        "payload": payload,
        "analysis_result": analysis_result,
        "execution_result": execution_result,
    }

    for source_name in source_precedence:
        if source_name not in candidates:
            raise ResultNormalizerError(
                f"Unsupported tabular source in precedence: {source_name}"
            )

        candidate = _extract_tabular_payload(candidates[source_name])
        if candidate is not None:
            return candidate

    return []


def _normalize_mapping(row: Mapping[object, object]) -> dict[str, object]:
    # preserve mapping values as-is; serialization step will coerce to JSON-safe types
    return {str(key): value for key, value in row.items()}


def _extract_tabular_payload(data: object) -> object:
    if data is None:
        return None

    if isinstance(data, Mapping):
        columns = _extract_column_names(data)
        for container_key in _TABULAR_CONTAINER_KEYS:
            if container_key not in data:
                continue

            nested = data[container_key]
            if columns is not None:
                rows = _normalize_rows_with_columns(nested, columns)
                if rows is not None:
                    return rows

            nested_tabular = _extract_tabular_payload(nested)
            if nested_tabular is not None:
                return nested_tabular

        if columns is not None:
            for container_key in _TABULAR_CONTAINER_KEYS:
                if container_key not in data:
                    continue
                rows = _normalize_rows_with_columns(data[container_key], columns)
                if rows is not None:
                    return rows

    try:
        _ = normalize_result(data)
    except ResultNormalizerError:
        pass
    else:
        return data

    return _extract_dataframe_records(data)


def _extract_column_names(data: Mapping[object, object]) -> list[str] | None:
    for key in _TABULAR_COLUMNS_KEYS:
        raw_columns = data.get(key)
        if not isinstance(raw_columns, Sequence) or isinstance(
            raw_columns, (str, bytes, bytearray)
        ):
            continue
        return [str(column) for column in raw_columns]
    return None


def _normalize_rows_with_columns(
    rows: object, columns: Sequence[str]
) -> list[dict[str, object]] | None:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return None

    normalized_rows: list[dict[str, object]] = []
    for row in rows:
        if isinstance(row, Mapping):
            normalized_rows.append(_normalize_mapping(row))
            continue
        if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
            normalized_rows.append(
                {
                    str(column): row[index] if index < len(row) else None
                    for index, column in enumerate(columns)
                }
            )
            continue
        return None
    return normalized_rows


def _extract_dataframe_records(data: object) -> object:
    for method_name in ("fetchdf", "df"):
        method = getattr(data, method_name, None)
        if not callable(method):
            continue
        try:
            frame = method()
        except Exception:
            continue
        try:
            _ = normalize_result(frame)
        except ResultNormalizerError:
            continue
        return frame
    return None


def _serialize_value(value: object) -> object:
    from decimal import Decimal
    import uuid
    from dataclasses import is_dataclass, asdict
    # avoid importing numpy at module import time; handle optional types

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("utf-8")
    if isinstance(value, bytearray):
        return base64.b64encode(bytes(value)).decode("utf-8")
    if isinstance(value, memoryview):
        return base64.b64encode(value.tobytes()).decode("utf-8")
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize_value(item) for item in value]
    if isinstance(value, Mapping):
        return {
            str(key): _serialize_value(item)
            for key, item in value.items()
        }

    # dataclass -> dict (only for instances)
    if is_dataclass(value) and not isinstance(value, type):
        return _serialize_value(asdict(value))

    # numpy scalars and arrays
    # handle numpy-like objects without importing numpy at module-load time
    try:
        # Some array/scalar-like objects expose item() and tolist(). Use getattr
        # and wrap in a narrow-typed check to keep diagnostics quieter.
        item_attr = getattr(value, "item", None)
        if callable(item_attr):
            py = item_attr()
            return _serialize_value(py)
        tolist_attr = getattr(value, "tolist", None)
        if callable(tolist_attr):
            arr_list = tolist_attr()
            if isinstance(arr_list, list):
                return [_serialize_value(item) for item in arr_list]
    except Exception:
        pass

    # attempt generic item() call for array-like scalars (ignore type checks)
    value_item = getattr(value, "item", None)  # type: ignore
    if callable(value_item):
        try:
            return _serialize_value(value_item())
        except (TypeError, ValueError):
            pass

    # sets -> sorted lists for determinism
    if isinstance(value, (set, frozenset)):
        try:
            # cast to list first and sort deterministically by string form to avoid
            # comparing unorderable elements (ensures deterministic output)
            sorted_items = sorted(list(value), key=lambda x: str(x))
        except TypeError:
            sorted_items = list(value)
        return [_serialize_value(item) for item in sorted_items]

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_serialize_value(item) for item in value]

    return str(value)


__all__ = [
    "ResultNormalizerError",
    "ensure_json_serializable",
    "normalize_and_serialize",
    "normalize_canonical_tabular_result",
    "normalize_result",
    "select_canonical_tabular_payload",
]
