"""Tests for the per-resource series read path.

Run inside the indicator-service container (which has motor / pydantic / bson):
    docker exec indicator-service python3 -m unittest tests.test_series_propagator -v

These tests mock the `db` object used by services.data_propagator so they
exercise only the merge / filter / sort / pagination / composition logic —
no MongoDB roundtrip.
"""

import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timezone
from bson.objectid import ObjectId

import services.data_propagator as propagator


def _segment(ts, points, indicator_id=None, resource_id=None):
    """Build a fake mongo doc for db.data_segments."""
    doc = {"timestamp": ts, "points": points}
    if indicator_id is not None:
        doc["indicator_id"] = indicator_id
    if resource_id is not None:
        doc["resource_id"] = resource_id
    return doc


def _cursor(docs):
    """Wrap a list as a motor-style cursor (we only use .to_list)."""
    cur = MagicMock()
    cur.to_list = AsyncMock(return_value=docs)
    return cur


def _indicator_doc(indicator_id, name="Indicator", child_ids=None, name_en=None):
    """Build a fake indicator doc."""
    doc = {
        "_id": ObjectId(indicator_id) if isinstance(indicator_id, str) else indicator_id,
        "name": name,
        "name_en": name_en or name,
        "deleted": False,
    }
    if child_ids:
        doc["child_indicators"] = list(child_ids)
    return doc


class MergeSegmentsTests(unittest.TestCase):
    """_merge_segments (sync): per-resource dedup keeps the latest segment."""

    def test_returns_empty_list_when_no_segments(self):
        self.assertEqual(propagator._merge_segments([]), [])

    def test_dedup_picks_latest_segment_per_x(self):
        x_val = datetime(2020, 1, 1)
        older = datetime(2020, 1, 1, 10, 0)
        newer = datetime(2020, 1, 2, 10, 0)
        # Newer timestamp wins regardless of order in input list.
        points = propagator._merge_segments([
            _segment(newer, [{"x": x_val, "y": 20.0}]),
            _segment(older, [{"x": x_val, "y": 10.0}]),
        ])
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].x, x_val)
        self.assertEqual(points[0].y, 20.0)

    def test_same_x_different_series_kept_separate(self):
        x_val = datetime(2020, 1, 1)
        ts = datetime(2024, 1, 1)
        points = propagator._merge_segments([
            _segment(ts, [
                {"x": x_val, "y": 100.0, "series": "Total"},
                {"x": x_val, "y": 50.0, "series": "Águas residuais"},
            ]),
        ])
        self.assertEqual(len(points), 2)
        self.assertEqual(
            sorted(p.series for p in points),
            ["Total", "Águas residuais"],
        )

    def test_keeps_distinct_x_values(self):
        x1 = datetime(2020, 1, 1)
        x2 = datetime(2021, 1, 1)
        ts = datetime(2024, 1, 1)
        points = propagator._merge_segments([
            _segment(ts, [{"x": x1, "y": 10.0}, {"x": x2, "y": 20.0}]),
        ])
        self.assertEqual([p.y for p in points], [10.0, 20.0])

    def test_parses_iso_string_x(self):
        points = propagator._merge_segments([
            _segment(datetime(2024, 1, 1), [{"x": "2020-01-01T00:00:00", "y": 5.0}]),
        ])
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].x, datetime(2020, 1, 1))


class GetSeriesDataPointsTests(unittest.IsolatedAsyncioTestCase):
    """get_series_data_points: one entry per (resource, series_label)."""

    def _build_db(self, segments_by_indicator, indicators=None):
        """Build a mock db that:
          - resolves db.indicators.find_one by ObjectId match
          - resolves db.data_segments.find by indicator_id match
        `segments_by_indicator` maps indicator_id (str) → list of segment dicts.
        Each segment dict must include `resource_id` (ObjectId).
        """
        mock_db = MagicMock()

        # Indicators collection: find_one looks up by _id.
        indicator_docs = list(indicators or [])
        # Auto-create stub indicator docs for any indicator_id that has segments
        # but no explicit indicator entry — keeps tests concise.
        seen_ids = {str(d["_id"]) for d in indicator_docs}
        for ind_id in segments_by_indicator.keys():
            if ind_id not in seen_ids:
                indicator_docs.append(_indicator_doc(ind_id))

        async def find_one_indicator(query):
            wanted = query.get("_id")
            for d in indicator_docs:
                if d["_id"] == wanted and not d.get("deleted", False):
                    return d
            return None

        mock_db.indicators.find_one = AsyncMock(side_effect=find_one_indicator)

        # Data segments collection: find filters by indicator_id.
        def find_segments(query):
            ind_oid = query.get("indicator_id")
            ind_str = str(ind_oid) if ind_oid is not None else None
            return _cursor(segments_by_indicator.get(ind_str, []))

        mock_db.data_segments.find = MagicMock(side_effect=find_segments)
        return mock_db

    async def test_one_series_per_resource_legacy(self):
        ind_id = "507f1f77bcf86cd799439099"
        rid_a = ObjectId("507f1f77bcf86cd799439010")
        rid_b = ObjectId("507f1f77bcf86cd799439011")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db({
            ind_id: [
                _segment(ts, [{"x": datetime(2020, 1, 1), "y": 1.0}], resource_id=rid_a),
                _segment(ts, [{"x": datetime(2020, 1, 1), "y": 2.0}], resource_id=rid_b),
            ],
        })

        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(ind_id)

        self.assertEqual(len(series), 2)
        for s in series:
            self.assertIsNone(s["series_label"])
            # Source indicator metadata is always populated.
            self.assertEqual(s["source_indicator_id"], ind_id)
        rids = sorted(s["resource_id"] for s in series)
        self.assertEqual(rids, sorted([str(rid_a), str(rid_b)]))
        ys = {s["resource_id"]: [p["y"] for p in s["points"]] for s in series}
        self.assertEqual(ys[str(rid_a)], [1.0])
        self.assertEqual(ys[str(rid_b)], [2.0])

    async def test_one_resource_many_series(self):
        ind_id = "507f1f77bcf86cd799439099"
        rid = ObjectId("507f1f77bcf86cd799439010")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db({
            ind_id: [_segment(ts, [
                {"x": datetime(2020, 1, 1), "y": 100.0, "series": "Total"},
                {"x": datetime(2020, 1, 1), "y": 50.0, "series": "Sub"},
                {"x": datetime(2021, 1, 1), "y": 110.0, "series": "Total"},
                {"x": datetime(2021, 1, 1), "y": 55.0, "series": "Sub"},
            ], resource_id=rid)],
        })

        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(ind_id)

        self.assertEqual(len(series), 2)
        labels = sorted(s["series_label"] for s in series)
        self.assertEqual(labels, ["Sub", "Total"])
        self.assertEqual({s["resource_id"] for s in series}, {str(rid)})
        by_label = {s["series_label"]: s for s in series}
        self.assertEqual([p["y"] for p in by_label["Total"]["points"]], [100.0, 110.0])
        self.assertEqual([p["y"] for p in by_label["Sub"]["points"]], [50.0, 55.0])

    async def test_iso_serialisation_of_x(self):
        ind_id = "507f1f77bcf86cd799439099"
        rid = ObjectId("507f1f77bcf86cd799439010")
        mock_db = self._build_db({
            ind_id: [_segment(datetime(2024, 1, 1),
                              [{"x": datetime(2020, 1, 1), "y": 1.0}],
                              resource_id=rid)],
        })
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(ind_id)
        self.assertEqual(series[0]["points"][0]["x"], "2020-01-01T00:00:00")

    async def test_date_filter(self):
        ind_id = "507f1f77bcf86cd799439099"
        rid = ObjectId("507f1f77bcf86cd799439010")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db({
            ind_id: [_segment(ts, [
                {"x": datetime(2019, 1, 1), "y": 1.0},
                {"x": datetime(2020, 1, 1), "y": 2.0},
                {"x": datetime(2021, 1, 1), "y": 3.0},
            ], resource_id=rid)],
        })
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(
                ind_id,
                start_date=datetime(2020, 1, 1),
                end_date=datetime(2020, 12, 31),
            )
        ys = [p["y"] for p in series[0]["points"]]
        self.assertEqual(ys, [2.0])

    async def test_sort_desc(self):
        ind_id = "507f1f77bcf86cd799439099"
        rid = ObjectId("507f1f77bcf86cd799439010")
        mock_db = self._build_db({
            ind_id: [_segment(datetime(2024, 1, 1), [
                {"x": datetime(2019, 1, 1), "y": 1.0},
                {"x": datetime(2020, 1, 1), "y": 2.0},
                {"x": datetime(2021, 1, 1), "y": 3.0},
            ], resource_id=rid)],
        })
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(ind_id, sort="desc")
        ys = [p["y"] for p in series[0]["points"]]
        self.assertEqual(ys, [3.0, 2.0, 1.0])

    async def test_limit_and_skip(self):
        ind_id = "507f1f77bcf86cd799439099"
        rid = ObjectId("507f1f77bcf86cd799439010")
        mock_db = self._build_db({
            ind_id: [_segment(datetime(2024, 1, 1), [
                {"x": datetime(2019, 1, 1), "y": 1.0},
                {"x": datetime(2020, 1, 1), "y": 2.0},
                {"x": datetime(2021, 1, 1), "y": 3.0},
                {"x": datetime(2022, 1, 1), "y": 4.0},
            ], resource_id=rid)],
        })
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(ind_id, skip=1, limit=2)
        ys = [p["y"] for p in series[0]["points"]]
        self.assertEqual(ys, [2.0, 3.0])

    async def test_empty_indicator_returns_empty_list(self):
        ind_id = "507f1f77bcf86cd799439099"
        mock_db = self._build_db({ind_id: []})
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(ind_id)
        self.assertEqual(series, [])

    async def test_timezone_aware_query_dates_dont_crash(self):
        ind_id = "507f1f77bcf86cd799439099"
        rid = ObjectId("507f1f77bcf86cd799439010")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db({
            ind_id: [_segment(ts, [
                {"x": datetime(2020, 1, 1), "y": 1.0},
                {"x": datetime(2021, 1, 1), "y": 2.0},
            ], resource_id=rid)],
        })
        end_aware = datetime(1993, 1, 1, tzinfo=timezone.utc)
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(ind_id, end_date=end_aware)
        self.assertEqual(series, [])


class ComposedIndicatorTests(unittest.IsolatedAsyncioTestCase):
    """Composition: a parent's /series response includes children's data
    transitively, every line tagged with its source indicator."""

    def _build_db(self, segments_by_indicator, indicators):
        mock_db = MagicMock()

        async def find_one_indicator(query):
            wanted = query.get("_id")
            for d in indicators:
                if d["_id"] == wanted and not d.get("deleted", False):
                    return d
            return None

        mock_db.indicators.find_one = AsyncMock(side_effect=find_one_indicator)

        def find_segments(query):
            ind_oid = query.get("indicator_id")
            ind_str = str(ind_oid) if ind_oid is not None else None
            return _cursor(segments_by_indicator.get(ind_str, []))

        mock_db.data_segments.find = MagicMock(side_effect=find_segments)
        return mock_db

    async def test_parent_with_one_child_aggregates_both(self):
        # A includes B. A has its own data; B has its own data. Chart should
        # show both lines, each tagged with its source.
        a_id = "507f1f77bcf86cd799439001"
        b_id = "507f1f77bcf86cd799439002"
        rid_a = ObjectId("507f1f77bcf86cd799439101")
        rid_b = ObjectId("507f1f77bcf86cd799439102")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db(
            segments_by_indicator={
                a_id: [_segment(ts, [{"x": datetime(2020, 1, 1), "y": 10.0}], resource_id=rid_a)],
                b_id: [_segment(ts, [{"x": datetime(2020, 1, 1), "y": 20.0}], resource_id=rid_b)],
            },
            indicators=[
                _indicator_doc(a_id, "Parent A", child_ids=[b_id]),
                _indicator_doc(b_id, "Child B"),
            ],
        )
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(a_id)

        self.assertEqual(len(series), 2)
        by_source = {s["source_indicator_id"]: s for s in series}
        self.assertIn(a_id, by_source)
        self.assertIn(b_id, by_source)
        self.assertEqual(by_source[a_id]["source_indicator_name"], "Parent A")
        self.assertEqual(by_source[b_id]["source_indicator_name"], "Child B")
        self.assertEqual(by_source[a_id]["resource_id"], str(rid_a))
        self.assertEqual(by_source[b_id]["resource_id"], str(rid_b))

    async def test_transitive_composition(self):
        # A → B → C. C has data. A's response should include C's series tagged
        # with C as source — flattening the entire subtree.
        a_id = "507f1f77bcf86cd799439001"
        b_id = "507f1f77bcf86cd799439002"
        c_id = "507f1f77bcf86cd799439003"
        rid_c = ObjectId("507f1f77bcf86cd799439103")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db(
            segments_by_indicator={
                c_id: [_segment(ts, [{"x": datetime(2020, 1, 1), "y": 30.0}], resource_id=rid_c)],
            },
            indicators=[
                _indicator_doc(a_id, "A", child_ids=[b_id]),
                _indicator_doc(b_id, "B", child_ids=[c_id]),
                _indicator_doc(c_id, "C"),
            ],
        )
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(a_id)

        self.assertEqual(len(series), 1)
        self.assertEqual(series[0]["source_indicator_id"], c_id)
        self.assertEqual(series[0]["source_indicator_name"], "C")
        self.assertEqual(series[0]["points"][0]["y"], 30.0)

    async def test_diamond_no_double_count(self):
        # A → B → D and A → C → D. D appears in two paths; the visited set
        # must keep it from being walked twice. Otherwise D's lines would
        # appear duplicated.
        a_id = "507f1f77bcf86cd799439001"
        b_id = "507f1f77bcf86cd799439002"
        c_id = "507f1f77bcf86cd799439003"
        d_id = "507f1f77bcf86cd799439004"
        rid_d = ObjectId("507f1f77bcf86cd799439104")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db(
            segments_by_indicator={
                d_id: [_segment(ts, [{"x": datetime(2020, 1, 1), "y": 99.0}], resource_id=rid_d)],
            },
            indicators=[
                _indicator_doc(a_id, "A", child_ids=[b_id, c_id]),
                _indicator_doc(b_id, "B", child_ids=[d_id]),
                _indicator_doc(c_id, "C", child_ids=[d_id]),
                _indicator_doc(d_id, "D"),
            ],
        )
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(a_id)

        # D's data must appear exactly once.
        d_series = [s for s in series if s["source_indicator_id"] == d_id]
        self.assertEqual(len(d_series), 1)

    async def test_cycle_does_not_loop(self):
        # If somehow A → B → A escaped the add-time cycle check (e.g. legacy
        # data), the runtime visited set must still terminate the walk
        # instead of recursing forever.
        a_id = "507f1f77bcf86cd799439001"
        b_id = "507f1f77bcf86cd799439002"
        rid_a = ObjectId("507f1f77bcf86cd799439101")
        rid_b = ObjectId("507f1f77bcf86cd799439102")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db(
            segments_by_indicator={
                a_id: [_segment(ts, [{"x": datetime(2020, 1, 1), "y": 1.0}], resource_id=rid_a)],
                b_id: [_segment(ts, [{"x": datetime(2020, 1, 1), "y": 2.0}], resource_id=rid_b)],
            },
            indicators=[
                _indicator_doc(a_id, "A", child_ids=[b_id]),
                _indicator_doc(b_id, "B", child_ids=[a_id]),  # back-edge
            ],
        )
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(a_id)

        # Each indicator contributes once.
        sources = sorted(s["source_indicator_id"] for s in series)
        self.assertEqual(sources, sorted([a_id, b_id]))

    async def test_missing_child_silently_skipped(self):
        # A includes B, but B's doc was deleted. The walk must not crash —
        # just skip B and return whatever data A has on its own.
        a_id = "507f1f77bcf86cd799439001"
        b_id = "507f1f77bcf86cd799439002"
        rid_a = ObjectId("507f1f77bcf86cd799439101")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db(
            segments_by_indicator={
                a_id: [_segment(ts, [{"x": datetime(2020, 1, 1), "y": 1.0}], resource_id=rid_a)],
            },
            # Only A's doc exists — B is absent.
            indicators=[_indicator_doc(a_id, "A", child_ids=[b_id])],
        )
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(a_id)

        self.assertEqual(len(series), 1)
        self.assertEqual(series[0]["source_indicator_id"], a_id)

    async def test_plain_indicator_still_tags_source(self):
        # Even with no children, every line carries source_indicator_*. The
        # frontend uses the count of distinct source IDs to decide whether to
        # show the prefix; for a non-composed indicator it sees only one.
        ind_id = "507f1f77bcf86cd799439099"
        rid = ObjectId("507f1f77bcf86cd799439010")
        ts = datetime(2024, 1, 1)
        mock_db = self._build_db(
            segments_by_indicator={
                ind_id: [_segment(ts, [{"x": datetime(2020, 1, 1), "y": 1.0}], resource_id=rid)],
            },
            indicators=[_indicator_doc(ind_id, "Solo")],
        )
        with patch.object(propagator, "db", mock_db):
            series = await propagator.get_series_data_points(ind_id)
        self.assertEqual(len(series), 1)
        self.assertEqual(series[0]["source_indicator_id"], ind_id)
        self.assertEqual(series[0]["source_indicator_name"], "Solo")


if __name__ == "__main__":
    unittest.main()
