"""Metric hierarchy abstractions for planner-side metric specs."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class MetricGranularity(str, Enum):
    """Supported geo/organization granularities for metric queries."""

    PROVINCE = "province"
    CITY = "city"
    DISTRICT = "district"
    SCHOOL = "school"


class MetricComputationMethod(str, Enum):
    """Supported computation methods for metric execution."""

    SUM = "sum"
    AVG = "avg"
    WEIGHTED = "weighted"
    NORMALIZED = "normalized"
    RANK = "rank"


@dataclass(frozen=True)
class MetricPath:
    """Multi-level metric path (L1/L2/L3)."""

    level1: str
    level2: str | None = None
    level3: str | None = None

    @classmethod
    def from_text(cls, metric_path: str) -> "MetricPath":
        parts = [part.strip() for part in metric_path.replace("＞", ">").split(">") if part.strip()]
        if not parts:
            raise ValueError("metric_path must contain at least one level")
        level1 = parts[0]
        level2 = parts[1] if len(parts) >= 2 else None
        level3 = parts[2] if len(parts) >= 3 else None
        return cls(level1=level1, level2=level2, level3=level3)

    def as_tuple(self) -> tuple[str, ...]:
        parts = [self.level1]
        if self.level2:
            parts.append(self.level2)
        if self.level3:
            parts.append(self.level3)
        return tuple(parts)

    def to_display(self) -> str:
        return " > ".join(self.as_tuple())


@dataclass(frozen=True)
class MetricComputation:
    """Metric computation mode and related strategy fields."""

    method: MetricComputationMethod = MetricComputationMethod.AVG
    weight_field: str | None = None
    normalization_strategy: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "method": self.method.value,
            "weight_field": self.weight_field,
            "normalization_strategy": self.normalization_strategy,
        }


@dataclass(frozen=True)
class MetricQuerySpec:
    """Hierarchy-driven planner input for metric queries."""

    metric_path: MetricPath
    granularity: MetricGranularity
    filters: list[dict[str, object]] = field(default_factory=list)
    computation: MetricComputation = field(default_factory=MetricComputation)
    normalization_strategy: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "metric_path": {
                "level1": self.metric_path.level1,
                "level2": self.metric_path.level2,
                "level3": self.metric_path.level3,
                "display": self.metric_path.to_display(),
            },
            "granularity": self.granularity.value,
            "filters": [dict(metric_filter) for metric_filter in self.filters],
            "computation": self.computation.to_dict(),
            "normalization_strategy": self.normalization_strategy,
        }


__all__ = [
    "MetricComputation",
    "MetricComputationMethod",
    "MetricGranularity",
    "MetricPath",
    "MetricQuerySpec",
]
