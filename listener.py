import json
import logging
import time

import psycopg2
import psycopg2.extensions

from database import get_connection

log = logging.getLogger(__name__)

# Seconds to wait before retrying after a lost connection.
_RECONNECT_DELAY_S = 5

# Seconds to sleep when the notify queue is empty, to avoid busy-waiting.
# Kept short so we don't miss a burst of rapid notifications.
_IDLE_SLEEP_S = 0.2


def _fetch_order(cur, order_id: int) -> dict | None:
    """
    Fetch a single order row by id using an existing cursor.

    Accepts a cursor rather than opening a new connection so the caller
    (the listen loop) can reuse the same connection that is already open
    and sitting idle between polls.  Opening a fresh connection per
    notification would work in a demo but wastes a Postgres backend slot
    and adds ~5 ms of TCP + auth overhead on every event.

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

    Calls conn.poll() to drain any pending notifications from the socket
    buffer, processes every queued notify before sleeping, then idles
    briefly when the queue is empty.  This ensures a burst of rapid
    inserts/updates is fully drained without the 200 ms gap that a
    naive sleep-first loop would introduce between batches.

    Raises psycopg2.OperationalError if the underlying connection drops,
    which the caller (start_listener) catches and uses as the signal to
    reconnect.
    """
    # Reuse a single cursor for all _fetch_order calls inside this loop.
    # The connection is AUTOCOMMIT so there is no open transaction to worry
    # about — each SELECT runs and commits immediately.
    cur = conn.cursor()

    try:
        while True:
            conn.poll()

            if conn.notifies:
                # Drain the entire queue before sleeping.
                # If we slept between individual notifications we would add
                # _IDLE_SLEEP_S of artificial latency per event during bursts.
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    _handle_notify(notify, cur, socketio)
            else:
                # Nothing arrived — sleep briefly to avoid spinning the CPU.
                time.sleep(_IDLE_SLEEP_S)

    finally:
        cur.close()


def _handle_notify(notify, cur, socketio) -> None:
    """
    Process a single pg_notify event: parse the payload, fetch the current
    order row, and emit an 'order_update' event to all WebSocket clients.

    Kept as a separate function so _listen_loop stays readable and so this
    logic is easy to unit-test in isolation.
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
        # The notifying transaction was rolled back before our SELECT ran.
        log.warning("Order #%s not found after notify — possible rollback", order_id)
        return

    order["event_type"] = event_type
    log.info("Emitting %s for order #%s (status=%s)", event_type, order_id, order["status"])
    socketio.emit("order_update", order)


def start_listener(socketio) -> None:
    """
    Entry point for the background listener thread.

    Opens a dedicated AUTOCOMMIT connection to PostgreSQL, registers on
    the 'order_updates' channel via LISTEN, then enters _listen_loop.

    AUTOCOMMIT is required because LISTEN/NOTIFY must not run inside a
    transaction block — psycopg2's default isolation level wraps every
    statement in a transaction, which would silently prevent notifications
    from being delivered.

    Reconnect behaviour: if the connection drops for any reason (Postgres
    restart, Docker stop, network blip), the outer while-loop catches the
    OperationalError, closes the dead connection, sleeps for
    _RECONNECT_DELAY_S seconds, and opens a fresh one.  The thread never
    exits on its own — it only stops when the main process is killed.
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