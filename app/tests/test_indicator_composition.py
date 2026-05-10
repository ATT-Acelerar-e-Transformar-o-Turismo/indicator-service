"""Tests for the composed-indicator service helpers (cycle detection,
add/remove child, get-children-with-deletion-filter).

Run inside the indicator-service container:
    docker exec indicator-service python3 -m unittest tests.test_indicator_composition -v
"""

import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from bson.objectid import ObjectId

import services.indicator_service as svc


# Three valid 24-hex ObjectIds for parent/child relationships.
A_ID = "507f1f77bcf86cd799439001"
B_ID = "507f1f77bcf86cd799439002"
C_ID = "507f1f77bcf86cd799439003"
D_ID = "507f1f77bcf86cd799439004"


def _doc(_id, child_ids=None, deleted=False, name="Ind"):
    return {
        "_id": ObjectId(_id) if isinstance(_id, str) else _id,
        "name": name,
        "deleted": deleted,
        "child_indicators": list(child_ids or []),
        "domain": ObjectId("507f1f77bcf86cd799439999"),
        "subdomain": "x",
    }


def _build_db(indicators):
    """Build a mock that resolves db.indicators.find_one and update_one against
    an in-memory list. update_one returns a result with matched_count=1 when
    a doc matches the query."""
    state = {d["_id"]: dict(d) for d in indicators}
    mock_db = MagicMock()

    async def find_one(query):
        wanted = query.get("_id")
        deleted_filter = query.get("deleted")
        for oid, d in state.items():
            if oid != wanted:
                continue
            if deleted_filter is False and d.get("deleted"):
                return None
            return dict(d)
        return None

    async def update_one(query, update):
        wanted = query.get("_id")
        deleted_filter = query.get("deleted")
        d = state.get(wanted)
        if not d:
            return MagicMock(matched_count=0, modified_count=0)
        if deleted_filter is False and d.get("deleted"):
            return MagicMock(matched_count=0, modified_count=0)
        modified = 0
        if "$addToSet" in update:
            for k, v in update["$addToSet"].items():
                arr = d.setdefault(k, [])
                if v not in arr:
                    arr.append(v)
                    modified = 1
        if "$pull" in update:
            for k, v in update["$pull"].items():
                arr = d.get(k, [])
                new_arr = [x for x in arr if x != v]
                if new_arr != arr:
                    d[k] = new_arr
                    modified = 1
        if "$set" in update:
            for k, v in update["$set"].items():
                d[k] = v
                modified = 1
        return MagicMock(matched_count=1, modified_count=modified)

    mock_db.indicators.find_one = AsyncMock(side_effect=find_one)
    mock_db.indicators.update_one = AsyncMock(side_effect=update_one)

    # find() used by get_child_indicators to filter deleted.
    def find(query, projection=None):
        ids = query.get("_id", {}).get("$in", [])
        deleted_filter = query.get("deleted")
        result = []
        for oid in ids:
            d = state.get(oid)
            if not d:
                continue
            if deleted_filter is False and d.get("deleted"):
                continue
            result.append({"_id": d["_id"]})
        cur = MagicMock()
        cur.to_list = AsyncMock(return_value=result)
        return cur

    mock_db.indicators.find = MagicMock(side_effect=find)

    # Need to also stub get_indicator_by_id's domain lookup. Domain doc must
    # exist for serialize() to work.
    async def find_one_domain(query):
        return {"_id": query.get("_id"), "name": "D", "deleted": False, "subdomains": []}

    mock_db.domains.find_one = AsyncMock(side_effect=find_one_domain)

    return mock_db, state


class CycleDetectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_self_is_not_descendant_of_self_via_visited(self):
        # _is_descendant uses visited so calling with candidate==ancestor must
        # short-circuit to True (the same-id case is the trivial cycle).
        mock_db, _ = _build_db([_doc(A_ID)])
        with patch.object(svc, "db", mock_db):
            result = await svc._is_descendant(A_ID, A_ID)
        self.assertTrue(result)

    async def test_unrelated_indicator_is_not_descendant(self):
        mock_db, _ = _build_db([_doc(A_ID), _doc(B_ID)])
        with patch.object(svc, "db", mock_db):
            self.assertFalse(await svc._is_descendant(A_ID, B_ID))

    async def test_transitive_descendant(self):
        # B → C → A means A is a descendant of B (cycle if B were added under A).
        mock_db, _ = _build_db([
            _doc(A_ID),
            _doc(B_ID, child_ids=[C_ID]),
            _doc(C_ID, child_ids=[A_ID]),
        ])
        with patch.object(svc, "db", mock_db):
            self.assertTrue(await svc._is_descendant(A_ID, B_ID))

    async def test_invalid_id_returns_false_not_crash(self):
        mock_db, _ = _build_db([])
        with patch.object(svc, "db", mock_db):
            self.assertFalse(await svc._is_descendant(A_ID, "not-an-objectid"))


class AddChildIndicatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_self_inclusion_rejected(self):
        mock_db, _ = _build_db([_doc(A_ID)])
        with patch.object(svc, "db", mock_db):
            with self.assertRaises(ValueError):
                await svc.add_child_indicator(A_ID, A_ID)

    async def test_missing_parent_rejected(self):
        mock_db, _ = _build_db([_doc(B_ID)])
        with patch.object(svc, "db", mock_db):
            with self.assertRaises(ValueError):
                await svc.add_child_indicator(A_ID, B_ID)

    async def test_missing_child_rejected(self):
        mock_db, _ = _build_db([_doc(A_ID)])
        with patch.object(svc, "db", mock_db):
            with self.assertRaises(ValueError):
                await svc.add_child_indicator(A_ID, B_ID)

    async def test_simple_cycle_rejected(self):
        # B already includes A; adding B as a child of A would close the cycle.
        mock_db, _ = _build_db([
            _doc(A_ID),
            _doc(B_ID, child_ids=[A_ID]),
        ])
        with patch.object(svc, "db", mock_db):
            with self.assertRaises(ValueError) as cm:
                await svc.add_child_indicator(A_ID, B_ID)
            self.assertIn("cycle", str(cm.exception).lower())

    async def test_transitive_cycle_rejected(self):
        # B → C → A. Adding B under A would close A → B → C → A.
        mock_db, _ = _build_db([
            _doc(A_ID),
            _doc(B_ID, child_ids=[C_ID]),
            _doc(C_ID, child_ids=[A_ID]),
        ])
        with patch.object(svc, "db", mock_db):
            with self.assertRaises(ValueError):
                await svc.add_child_indicator(A_ID, B_ID)

    async def test_valid_add_persists(self):
        mock_db, state = _build_db([_doc(A_ID), _doc(B_ID)])
        with patch.object(svc, "db", mock_db):
            await svc.add_child_indicator(A_ID, B_ID)
        a_doc = state[ObjectId(A_ID)]
        self.assertIn(B_ID, a_doc["child_indicators"])

    async def test_idempotent_add(self):
        # Adding the same child twice must not duplicate the entry.
        mock_db, state = _build_db([
            _doc(A_ID, child_ids=[B_ID]),
            _doc(B_ID),
        ])
        with patch.object(svc, "db", mock_db):
            await svc.add_child_indicator(A_ID, B_ID)
        a_doc = state[ObjectId(A_ID)]
        self.assertEqual(a_doc["child_indicators"].count(B_ID), 1)

    async def test_diamond_allowed(self):
        # A → B; we now also add A → C where C → D and B → D. Diamond, not a
        # cycle. Should succeed.
        mock_db, state = _build_db([
            _doc(A_ID, child_ids=[B_ID]),
            _doc(B_ID, child_ids=[D_ID]),
            _doc(C_ID, child_ids=[D_ID]),
            _doc(D_ID),
        ])
        with patch.object(svc, "db", mock_db):
            await svc.add_child_indicator(A_ID, C_ID)
        a_doc = state[ObjectId(A_ID)]
        self.assertEqual(sorted(a_doc["child_indicators"]), sorted([B_ID, C_ID]))


class RemoveChildIndicatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_remove_present_child(self):
        mock_db, state = _build_db([
            _doc(A_ID, child_ids=[B_ID, C_ID]),
            _doc(B_ID),
            _doc(C_ID),
        ])
        with patch.object(svc, "db", mock_db):
            await svc.remove_child_indicator(A_ID, B_ID)
        a_doc = state[ObjectId(A_ID)]
        self.assertEqual(a_doc["child_indicators"], [C_ID])

    async def test_remove_absent_child_is_noop(self):
        # Removing a child that isn't there must not error.
        mock_db, state = _build_db([_doc(A_ID, child_ids=[B_ID]), _doc(B_ID), _doc(C_ID)])
        with patch.object(svc, "db", mock_db):
            await svc.remove_child_indicator(A_ID, C_ID)
        a_doc = state[ObjectId(A_ID)]
        self.assertEqual(a_doc["child_indicators"], [B_ID])


class GetChildIndicatorsTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_only_alive_children(self):
        # Children that have been (soft-)deleted must be filtered out.
        mock_db, _ = _build_db([
            _doc(A_ID, child_ids=[B_ID, C_ID]),
            _doc(B_ID),
            _doc(C_ID, deleted=True),
        ])
        with patch.object(svc, "db", mock_db):
            ids = await svc.get_child_indicators(A_ID)
        self.assertEqual(ids, [B_ID])

    async def test_returns_empty_when_none(self):
        mock_db, _ = _build_db([_doc(A_ID)])
        with patch.object(svc, "db", mock_db):
            ids = await svc.get_child_indicators(A_ID)
        self.assertEqual(ids, [])

    async def test_missing_parent_returns_empty(self):
        mock_db, _ = _build_db([])
        with patch.object(svc, "db", mock_db):
            ids = await svc.get_child_indicators(A_ID)
        self.assertEqual(ids, [])

    async def test_one_bad_id_does_not_skip_filter_for_others(self):
        # Regression: previously, if any entry in child_indicators failed
        # ObjectId() conversion, the function returned the raw list unfiltered
        # — leaking deleted IDs and the bad entry itself.
        mock_db, state = _build_db([
            _doc(A_ID),
            _doc(B_ID),
            _doc(C_ID, deleted=True),
        ])
        # Inject a malformed entry alongside valid ones.
        state[ObjectId(A_ID)]["child_indicators"] = [B_ID, "garbage", C_ID]
        with patch.object(svc, "db", mock_db):
            ids = await svc.get_child_indicators(A_ID)
        # Only B remains: the garbage entry is dropped, C is filtered as
        # deleted.
        self.assertEqual(ids, [B_ID])


class UpdateBypassPreventionTests(unittest.IsolatedAsyncioTestCase):
    """PUT/PATCH/POST routes go through update_indicator / create_indicator;
    those generic paths must NOT touch child_indicators (the dedicated
    endpoints run the cycle checks). Confirm both strip the field defensively.
    """

    async def test_update_indicator_strips_child_indicators(self):
        mock_db = MagicMock()
        captured = {}

        async def update_one(query, update):
            captured["update"] = update
            return MagicMock(matched_count=1, modified_count=1)

        mock_db.indicators.update_one = AsyncMock(side_effect=update_one)
        with patch.object(svc, "db", mock_db):
            await svc.update_indicator(A_ID, {
                "name": "renamed",
                "child_indicators": [B_ID],  # bypass attempt
            })

        # The $set payload must NOT include child_indicators.
        set_payload = captured["update"].get("$set", {})
        self.assertIn("name", set_payload)
        self.assertNotIn("child_indicators", set_payload)

    async def test_create_indicator_strips_child_indicators(self):
        from schemas.indicator import IndicatorCreate

        mock_db = MagicMock()
        inserted_doc = {}

        async def insert_one(doc):
            inserted_doc.update(doc)
            return MagicMock(inserted_id=doc["_id"])

        async def find_one_domain(query):
            return {"_id": query["_id"], "name": "D",
                    "subdomains": [{"name": "sub"}], "deleted": False}

        async def find_one_indicator(query):
            return {**inserted_doc, "domain": ObjectId("507f1f77bcf86cd799439999")}

        mock_db.indicators.insert_one = AsyncMock(side_effect=insert_one)
        mock_db.indicators.find_one = AsyncMock(side_effect=find_one_indicator)
        mock_db.domains.find_one = AsyncMock(side_effect=find_one_domain)

        # Build a minimal IndicatorCreate. The default chart_types include
        # 'line', so default_chart_type='line' satisfies the validator.
        create = IndicatorCreate(
            name="x", periodicity="annual", favourites=0, governance=False,
            child_indicators=[B_ID],  # bypass attempt
        )

        with patch.object(svc, "db", mock_db):
            await svc.create_indicator(
                "507f1f77bcf86cd799439999", "sub", create,
            )

        # The persisted doc must NOT carry child_indicators from the payload.
        self.assertNotIn("child_indicators", inserted_doc)


if __name__ == "__main__":
    unittest.main()
