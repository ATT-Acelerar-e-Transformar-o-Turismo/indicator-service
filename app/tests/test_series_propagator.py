"""Tests for the per-resource series read path added in Phase 1.

Run inside the indicator-service container (which has motor / pydantic / bson):
    docker exec indicator-service python3 -m unittest tests.test_series_propagator -v

These tests mock the `db` object used by services.data_propagator so they exercise
only the merge / filter / sort / pagination logic — no MongoDB roundtrip.
"""

import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime
from bson.objectid import ObjectId

import services.data_propagator as propagator


def _segment(ts, points):
    """Build a fake mongo doc for db.data_segments."""
    return {"timestamp": ts, "points": points}


def _cursor(docs):
    """Wrap a list as a motor-style cursor (we only use .to_list)."""
    cur = MagicMock()
    cur.to_list = AsyncMock(return_value=docs)
    return cur


class MergeResourceSegmentsTests(unittest.IsolatedAsyncioTestCase):
    """_merge_resource_segments: per-resource dedup keeps the latest segment."""

    async def test_returns_empty_list_when_no_segments(self):
        mock_db = MagicMock()
        mock_db.data_segments.find = MagicMock(return_value=_cursor([]))

        with patch.object(propagator, "db", mock_db):
            points = await propagator._merge_resource_segments(
                "507f1f77bcf86cd799439011", ObjectId("507f1f77bcf86cd799439012")
            )

        self.assertEqual(points, [])

    async def test_dedup_picks_latest_segment_per_x(self):
        x_val = datetime(2020, 1, 1)
        older = datetime(2020, 1, 1, 10, 0)
        newer = datetime(2020, 1, 2, 10, 0)

        mock_db = MagicMock()
        # Two segments overlap on the same x. Newer timestamp must win even
        # if it appears earlier in the result list (order not guaranteed).
        mock_db.data_segments.find = MagicMock(return_value=_cursor([
            _segment(newer, [{"x": x_val, "y": 20.0}]),
            _segment(older, [{"x": x_val, "y": 10.0}]),
        ]))

        with patch.object(propagator, "db", mock_db):
            points = await propagator._merge_resource_segments(
                "507f1f77bcf86cd799439011", ObjectId("507f1f77bcf86cd799439012")
            )

        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].x, x_val)
        self.assertEqual(points[0].y, 20.0)

    async def test_same_x_different_series_kept_separate(self):
        # Multi-column wrappers emit multiple points at the same x — one per
        # column. The dedup must NOT collapse them.
        x_val = datetime(2020, 1, 1)
        ts = datetime(2024, 1, 1)
        mock_db = MagicMock()
        mock_db.data_segments.find = MagicMock(return_value=_cursor([
            _segment(ts, [
                {"x": x_val, "y": 100.0, "series": "Total"},
                {"x": x_val, "y": 50.0, "series": "Águas residuais"},
            ]),
        ]))

        with patch.object(propagator, "db", mock_db):
            points = await propagator._merge_resource_segments(
                "507f1f77bcf86cd799439011", ObjectId("507f1f77bcf86cd799439012")
            )

        self.assertEqual(len(points), 2)
        labels = sorted(p.series for p in points)
        self.assertEqual(labels, ["Total", "Águas residuais"])

    async def test_keeps_distinct_x_values(self):
        x1 = datetime(2020, 1, 1)
        x2 = datetime(2021, 1, 1)
        ts = datetime(2024, 1, 1)

        mock_db = MagicMock()
        mock_db.data_segments.find = MagicMock(return_value=_cursor([
            _segment(ts, [
                {"x": x1, "y": 10.0},
                {"x": x2, "y": 20.0},
            ]),
        ]))

        with patch.object(propagator, "db", mock_db):
            points = await propagator._merge_resource_segments(
                "507f1f77bcf86cd799439011", ObjectId("507f1f77bcf86cd799439012")
            )

        self.assertEqual([p.y for p in points], [10.0, 20.0])  # sorted by x

    async def test_parses_iso_string_x(self):
        # Wrappers serialise x as ISO strings; merge must accept both.
        mock_db = MagicMock()
        mock_db.data_segments.find = MagicMock(return_value=_cursor([
            _segment(datetime(2024, 1, 1), [{"x": "2020-01-01T00:00:00", "y": 5.0}]),
        ]))

        with patch.object(propagator, "db", mock_db):
            points = await propagator._merge_resource_segments(
                "507f1f77bcf86cd799439011", ObjectId("507f1f77bcf86cd799439012")
            )

        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].x, datetime(2020, 1, 1))


class GetSeriesDataPointsTests(unittest.IsolatedAsyncioTestCase):
    """get_series_data_points: one entry per resource, filters & sort applied."""

    def _build_db(self, segments_by_resource):
        """Build a mock db where distinct() returns the resource IDs and
        find() filters by resource_id from the in-memory map."""
        mock_db = MagicMock()
        mock_db.data_segments.distinct = AsyncMock(
            return_value=list(segments_by_resource.keys())
        )

        def find(query):
            rid = query.get("resource_id")
            return _cursor(segments_by_resource.get(rid, []))

        mock_db.data_segments.find = MagicMock(side_effect=find)
        return mock_db

    async def test_one_series_per_resource_legacy(self):
        # Series-less data (legacy single-stream wrappers) → one entry per
        # resource with series_label=None.
        rid_a = ObjectId("507f1f77bcf86cd799439010")
        rid_b = ObjectId("507f1f77bcf86cd799439011")
        ts = datetime(2024, 1, 1)

        mock_db = self._build_db({
            rid_a: [_segment(ts, [{"x": datetime(2020, 1, 1), "y": 1.0}])],
            rid_b: [_segment(ts, [{"x": datetime(2020, 1, 1), "y": 2.0}])],
        })

        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points("507f1f77bcf86cd799439099")

        self.assertEqual(len(series), 2)
        # Each entry has resource_id (stringified), series_label, and points
        for s in series:
            self.assertIsNone(s["series_label"])
        rids = sorted(s["resource_id"] for s in series)
        self.assertEqual(rids, sorted([str(rid_a), str(rid_b)]))
        # Values stay separate per resource (we don't merge across resources)
        ys = {s["resource_id"]: [p["y"] for p in s["points"]] for s in series}
        self.assertEqual(ys[str(rid_a)], [1.0])
        self.assertEqual(ys[str(rid_b)], [2.0])

    async def test_one_resource_many_series(self):
        # The new multi-column path: one resource emits N series labels and
        # /series should split them into N entries.
        rid = ObjectId("507f1f77bcf86cd799439010")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db({
            rid: [_segment(ts, [
                {"x": datetime(2020, 1, 1), "y": 100.0, "series": "Total"},
                {"x": datetime(2020, 1, 1), "y": 50.0, "series": "Gestão de águas residuais"},
                {"x": datetime(2021, 1, 1), "y": 110.0, "series": "Total"},
                {"x": datetime(2021, 1, 1), "y": 55.0, "series": "Gestão de águas residuais"},
            ])]
        })

        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points("507f1f77bcf86cd799439099")

        self.assertEqual(len(series), 2)
        labels = sorted(s["series_label"] for s in series)
        self.assertEqual(labels, ["Gestão de águas residuais", "Total"])
        by_label = {s["series_label"]: s for s in series}
        # Both series belong to the same resource_id
        self.assertEqual({s["resource_id"] for s in series}, {str(rid)})
        # Each series carries only its own data points (not co-mingled)
        self.assertEqual([p["y"] for p in by_label["Total"]["points"]], [100.0, 110.0])
        self.assertEqual([p["y"] for p in by_label["Gestão de águas residuais"]["points"]], [50.0, 55.0])

    async def test_iso_serialisation_of_x(self):
        rid = ObjectId("507f1f77bcf86cd799439010")
        mock_db = self._build_db({
            rid: [_segment(datetime(2024, 1, 1), [{"x": datetime(2020, 1, 1), "y": 1.0}])]
        })
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points("507f1f77bcf86cd799439099")
        self.assertEqual(series[0]["points"][0]["x"], "2020-01-01T00:00:00")

    async def test_date_filter(self):
        rid = ObjectId("507f1f77bcf86cd799439010")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db({
            rid: [_segment(ts, [
                {"x": datetime(2019, 1, 1), "y": 1.0},
                {"x": datetime(2020, 1, 1), "y": 2.0},
                {"x": datetime(2021, 1, 1), "y": 3.0},
            ])]
        })

        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(
                "507f1f77bcf86cd799439099",
                start_date=datetime(2020, 1, 1),
                end_date=datetime(2020, 12, 31),
            )

        ys = [p["y"] for p in series[0]["points"]]
        self.assertEqual(ys, [2.0])

    async def test_sort_desc(self):
        rid = ObjectId("507f1f77bcf86cd799439010")
        mock_db = self._build_db({
            rid: [_segment(datetime(2024, 1, 1), [
                {"x": datetime(2019, 1, 1), "y": 1.0},
                {"x": datetime(2020, 1, 1), "y": 2.0},
                {"x": datetime(2021, 1, 1), "y": 3.0},
            ])]
        })

        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(
                "507f1f77bcf86cd799439099", sort="desc"
            )

        ys = [p["y"] for p in series[0]["points"]]
        self.assertEqual(ys, [3.0, 2.0, 1.0])

    async def test_limit_and_skip(self):
        rid = ObjectId("507f1f77bcf86cd799439010")
        mock_db = self._build_db({
            rid: [_segment(datetime(2024, 1, 1), [
                {"x": datetime(2019, 1, 1), "y": 1.0},
                {"x": datetime(2020, 1, 1), "y": 2.0},
                {"x": datetime(2021, 1, 1), "y": 3.0},
                {"x": datetime(2022, 1, 1), "y": 4.0},
            ])]
        })

        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(
                "507f1f77bcf86cd799439099", skip=1, limit=2
            )

        ys = [p["y"] for p in series[0]["points"]]
        self.assertEqual(ys, [2.0, 3.0])

    async def test_empty_indicator_returns_empty_list(self):
        mock_db = self._build_db({})  # no resources have data
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points("507f1f77bcf86cd799439099")
        self.assertEqual(series, [])


if __name__ == "__main__":
    unittest.main()
