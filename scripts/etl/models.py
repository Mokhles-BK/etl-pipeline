"""Pydantic models for the normalized records the pipeline ingests.

These sit between the raw API JSON and the staging tables. Validating here
means a malformed record is rejected at the edge, never half-written.

The single committed model is Earthquake (USGS GeoJSON feature). The natural
key is the event id (e.g. "us6000twys") and the incremental cursor is the
event `time` (epoch milliseconds).

A GeoJSON feature nests most fields under `properties` with `id` and
`geometry` at the top level. The `_from_geojson_feature` validator merges
them into one flat record so the loader and the rest of the pipeline never
have to know about the nesting.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from pydantic.alias_generators import to_camel


class Geometry(BaseModel):
    """GeoJSON Point geometry: [longitude, latitude, depth]."""

    model_config = ConfigDict(extra="allow")

    type: str | None = None
    coordinates: list[float] | None = None


class Earthquake(BaseModel):
    """A single USGS earthquake event, validated from a GeoJSON feature."""

    model_config = ConfigDict(extra="allow", populate_by_name=True, alias_generator=to_camel)

    id: str
    mag: float
    mag_type: str | None = None
    place: str | None = None
    time: int
    geometry: Geometry | None = None
    latitude: float | None = None
    longitude: float | None = None
    depth: float | None = None
    status: str | None = None
    sig: float | None = None
    net: str | None = None
    nst: int | None = None
    tsunami: float | None = None
    alert: str | None = None
    cdi: float | None = None
    mmi: float | None = None
    gap: float | None = None
    dmin: float | None = None
    rms: float | None = None
    felt: float | None = None
    type: str | None = None
    title: str | None = None
    url: str | None = None
    detail: str | None = None
    updated: int | None = None

    @model_validator(mode="before")
    @classmethod
    def _from_geojson_feature(cls, data: object) -> object:
        """Flatten a GeoJSON feature: merge `properties` to the top level."""
        if isinstance(data, dict) and isinstance(data.get("properties"), dict):
            merged = dict(data["properties"])
            # id / geometry live at the feature level, not in properties.
            if "id" not in merged:
                merged["id"] = data.get("id")
            if "geometry" not in merged:
                merged["geometry"] = data.get("geometry")
            return merged
        return data

    @model_validator(mode="after")
    def _extract_coordinates(self) -> "Earthquake":
        """Pull latitude/longitude/depth out of geometry.coordinates.

        GeoJSON stores [longitude, latitude, depth]; the staging columns are
        named by coordinate, so we expose them explicitly rather than forcing
        every consumer to remember the ordering.
        """
        if self.geometry and self.geometry.coordinates:
            coords = self.geometry.coordinates
            if len(coords) > 0 and self.longitude is None:
                self.longitude = coords[0]
            if len(coords) > 1 and self.latitude is None:
                self.latitude = coords[1]
            if len(coords) > 2 and self.depth is None:
                self.depth = coords[2]
        return self

    @field_validator("mag", mode="before")
    @classmethod
    def _reject_null_magnitude(cls, value: object) -> object:
        # A missing/null magnitude is a data-quality failure, not a nullable
        # field — route it to staging.load_errors instead of writing NULL.
        if value is None:
            raise ValueError("magnitude is required")
        return value


class NormalizedRecord(BaseModel):
    """A single record tagged with the entity it belongs to."""

    model_config = ConfigDict(extra="allow")

    entity: str
    record: dict