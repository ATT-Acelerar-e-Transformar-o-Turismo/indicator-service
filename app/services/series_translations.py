"""Consumer for column-translation events from the resource-service Gemini
sidecar. Merges PT/EN labels onto the indicator(s) attached to a resource.

The wrapper-time translation is a seed: existing entries in
indicator.series_translations are preserved on conflict so admin overrides
(set via the indicator wizard) are never overwritten by a later
re-generation.
"""

import json
import logging

import aio_pika

from config import settings
from dependencies.database import db
from dependencies.rabbitmq import consumer


logger = logging.getLogger(__name__)


@consumer(settings.WRAPPER_TRANSLATIONS_QUEUE)
async def process_wrapper_translations(
    message: aio_pika.abc.AbstractIncomingMessage,
):
    async with message.process():
        try:
            payload = json.loads(message.body.decode())
        except json.JSONDecodeError:
            logger.error("Invalid JSON in wrapper-translations message")
            return

        resource_id = payload.get("resource_id")
        translations = payload.get("translations") or {}
        if not resource_id or not isinstance(translations, dict) or not translations:
            return

        indicators = await db.indicators.find(
            {"resources": resource_id, "deleted": False}
        ).to_list(length=None)
        if not indicators:
            logger.info(f"No indicators linked to resource {resource_id} for translations")
            return

        for indicator in indicators:
            indicator_id = indicator["_id"]
            existing = indicator.get("series_translations") or {}
            merged = dict(existing)
            for label, entry in translations.items():
                if not isinstance(entry, dict):
                    continue
                # Don't overwrite admin-edited values (anything already in
                # the indicator's stored translations stays). New labels and
                # labels that haven't been touched get the auto-translation.
                if label in merged:
                    continue
                merged[label] = {
                    "pt": str(entry.get("pt") or "").strip(),
                    "en": str(entry.get("en") or "").strip(),
                }

            if merged != existing:
                await db.indicators.update_one(
                    {"_id": indicator_id},
                    {"$set": {"series_translations": merged}},
                )
                logger.info(
                    f"Seeded {len(merged) - len(existing)} translations on indicator {indicator_id}"
                )
