"""Conflating (last-value) subscriber middleware for ChatNodes.

A ChatNode does one slow LLM call per message. When the agent enqueues
prompts onto ``ai_prompted.<name>`` faster than the model can answer them, a
backlog builds and the node ends up reasoning over prompts that are minutes to
hours stale — the dashboard's "Latest Reasoning" then freezes on whatever the
node last managed to finish.

This middleware turns the prompt topic into a conflating queue: when a message
is picked up, we compare its offset against the partition's log-end offset
(``highwater``, cached locally — no network round-trip). If a newer prompt is
already sitting behind it, the stale one is acked and dropped without invoking
the model. The node therefore only ever runs inference on the freshest prompt
per partition, so the effective in-flight depth collapses to ~1 per partition.

It also self-heals an existing backlog: on startup every message behind the
partition tail is skipped in milliseconds until the node catches up to live.
"""

import logging

from aiokafka import TopicPartition
from faststream.exceptions import AckMessage

# The ChatNode runs as a subprocess that never configures root logging at INFO,
# so a bare getLogger().info() would be dropped by Python's WARNING fallback.
# Give this logger its own stdout handler (captured + prefixed by agent_launcher)
# and disable propagation so it can't double-print through FastStream's handlers.
logger = logging.getLogger("conflation")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


async def conflate_stale_prompts(call_next, msg):
    """Drop a prompt if a newer one already exists on the same partition.

    Registered as a FastStream subscriber middleware, so it receives the
    ``KafkaMessage`` (which exposes ``.consumer`` and ``.raw_message``) and the
    downstream ``call_next``. Fails open: any bookkeeping error falls through to
    normal processing rather than blocking real work.
    """
    record = msg.raw_message
    # Non-batch subscriber yields a single ConsumerRecord; guard batch mode.
    if isinstance(record, (tuple, list)):
        return await call_next(msg)

    try:
        tp = TopicPartition(record.topic, record.partition)
        highwater = msg.consumer.highwater(tp)
    except Exception:  # noqa: BLE001 — never let conflation block inference
        return await call_next(msg)

    # highwater is the next offset to be produced; the freshest existing record
    # is at highwater - 1. Anything before that has been superseded.
    if highwater is not None and record.offset < highwater - 1:
        logger.info(
            "Skipping stale prompt on %s[%d] offset=%d (highwater=%d, lag=%d)",
            record.topic,
            record.partition,
            record.offset,
            highwater,
            highwater - 1 - record.offset,
        )
        # Ack so the offset advances and nothing is published downstream.
        raise AckMessage()

    return await call_next(msg)
