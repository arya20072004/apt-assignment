import json
import logging
import time

import psycopg2
import psycopg2.extensions

from database import get_connection

log = logging.getLogger(__name__)

_RECONNECT_DELAY_S = 5
_IDLE_SLEEP_S = 0.2


def _fetch_order(cur, order_id: int) -> dict | None:
    """
    Fetch a single order row by id using an existing cursor.

    Accepts a cursor rather than opening a new connection so the caller
    can reuse the connection already open between polls.  Opening a fresh
    connection per notification wastes a Postgres backend slot and adds
    TCP + auth overhead on every event.

    Returns None if the row is missing — this can happen if a transaction
    that triggered the notify was rolled back before we ran the SELECT.
    """
    cur.execute(
        """
        SELECT id, customer_name, product_name, status, created_at, updated_at
        FROM   orders
        WHERE  id = %s;
        """,
        (order_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None

    return {
        "id":            row[0],
        "customer_name": row[1],
        "product_name":  row[2],
        "status":        row[3],
        "created_at":    str(row[4]),
        "updated_at":    str(row[5]),
    }


def _listen_loop(conn, socketio) -> None:
    """
    Core event loop for a live LISTEN connection.

    Drains the full notification queue on each poll before sleeping,
    ensuring a burst of rapid inserts is processed without artificial
    per-event latency.

    Raises psycopg2.OperationalError if the connection drops, which
    start_listener catches as the signal to reconnect.
    """
    # Single cursor reused for all _fetch_order calls.
    # AUTOCOMMIT means no open transaction — each SELECT is immediate.
    cur = conn.cursor()

    try:
        while True:
            conn.poll()

            if conn.notifies:
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    _handle_notify(notify, cur, socketio)
            else:
                time.sleep(_IDLE_SLEEP_S)

    finally:
        cur.close()


def _handle_notify(notify, cur, socketio) -> None:
    """
    Process a single pg_notify event: parse the payload, fetch the current
    order row, and emit an 'order_update' event to all WebSocket clients.

    Separate function so _listen_loop stays readable and this logic
    is independently testable without a real Postgres connection.
    """
    try:
        meta = json.loads(notify.payload)
    except json.JSONDecodeError:
        log.warning("Dropping non-JSON notify payload: %r", notify.payload)
        return

    order_id   = meta.get("id")
    event_type = meta.get("event_type", "unknown")

    if order_id is None:
        log.warning("Notify payload missing 'id' field: %s", meta)
        return

    order = _fetch_order(cur, order_id)

    if order is None:
        log.warning("Order #%s not found after notify — possible rollback", order_id)
        return

    order["event_type"] = event_type
    log.info("Emitting %s for order #%s (status=%s)", event_type, order_id, order["status"])
    socketio.emit("order_update", order, namespace="/")


def start_listener(socketio) -> None:
    """
    Entry point for the background listener thread.

    Opens a dedicated AUTOCOMMIT connection to PostgreSQL, registers on
    the 'order_updates' channel via LISTEN, then enters _listen_loop.

    AUTOCOMMIT is required because LISTEN/NOTIFY must not run inside a
    transaction block — psycopg2's default isolation level wraps every
    statement in a transaction, silently preventing notification delivery.

    If the connection drops, the outer loop catches OperationalError,
    closes the dead connection, and reconnects after _RECONNECT_DELAY_S.
    The thread never exits on its own.
    """
    while True:
        conn = None
        try:
            conn = get_connection()
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as cur:
                cur.execute("LISTEN order_updates;")

            log.info("Listener ready — subscribed to 'order_updates'")
            print("Listening for database changes...")

            _listen_loop(conn, socketio)

        except psycopg2.OperationalError as exc:
            log.error(
                "Listener lost connection: %s  —  retrying in %ds",
                exc, _RECONNECT_DELAY_S,
            )

        except Exception as exc:
            # Catch-all so an unexpected bug doesn't silently kill the thread.
            log.exception("Unexpected error in listener loop: %s", exc)

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

        time.sleep(_RECONNECT_DELAY_S)