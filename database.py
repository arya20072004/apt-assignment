import os
import psycopg2

# ---------------------------------------------------------------------------
# Connection config
# ---------------------------------------------------------------------------
# Reads from environment variables so credentials are never hardcoded.
# Falls back to the Docker dev defaults for local development convenience.
# In any real deployment, set these via your process environment or a
# secrets manager — never commit actual credentials to version control.
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "dbname":   os.environ.get("DB_NAME",     "aptdb"),
    "user":     os.environ.get("DB_USER",     "aptuser"),
    "password": os.environ.get("DB_PASSWORD", "aptpassword"),
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
}


def get_connection() -> psycopg2.extensions.connection:
    """Open and return a new psycopg2 connection using DB_CONFIG."""
    return psycopg2.connect(**DB_CONFIG)


def setup_database() -> None:
    """
    Idempotently create all schema objects required by the application.

    Safe to call on every startup — every statement uses IF NOT EXISTS or
    CREATE OR REPLACE, so re-running against an already-configured database
    is a no-op.

    Objects created:
      - orders        : main orders table
      - order_events  : append-only audit log of every status transition
      - notify_order_change() : trigger function that writes to order_events
                                and fires pg_notify on the 'order_updates' channel
      - order_change_trigger  : AFTER INSERT OR UPDATE trigger on orders
    """
    try:
        conn = get_connection()
    except psycopg2.OperationalError as exc:
        # Surface a clean message instead of a raw traceback that could
        # expose connection details in a CI log or crash report.
        raise RuntimeError(
            f"Cannot connect to PostgreSQL at "
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']} "
            f"(db={DB_CONFIG['dbname']}, user={DB_CONFIG['user']}). "
            f"Is the database running?  Original error: {exc}"
        ) from exc

    try:
        cur = conn.cursor()

        # ── orders ───────────────────────────────────────────────────
        # created_at  — set once on INSERT, never changes.
        # updated_at  — refreshed on every UPDATE.
        # Keeping both lets you calculate how long an order sat in each
        # state, which is useful for SLA reporting.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id            SERIAL       PRIMARY KEY,
                customer_name VARCHAR(100) NOT NULL,
                product_name  VARCHAR(100) NOT NULL,
                status        VARCHAR(20)  NOT NULL
                                  CHECK (status IN ('pending', 'shipped', 'delivered')),
                created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # ── order_events (audit log) ─────────────────────────────────
        # Append-only table — rows are never updated or deleted.
        # Records every status transition so you have a full history of
        # what happened to an order and when.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_events (
                id          SERIAL      PRIMARY KEY,
                order_id    INT         NOT NULL REFERENCES orders(id),
                event_type  VARCHAR(20) NOT NULL,  -- 'insert' | 'update'
                old_status  VARCHAR(20),            -- NULL on initial insert
                new_status  VARCHAR(20) NOT NULL,
                occurred_at TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Index on order_id so GET /orders/<id>/history is an index seek,
        # not a sequential scan.  FK constraints in Postgres do NOT create
        # an index automatically — you have to add it explicitly.
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_order_events_order_id
                ON order_events (order_id);
        """)

        # ── trigger function ─────────────────────────────────────────
        # Two responsibilities:
        #   1. Write an audit row to order_events.
        #   2. Fire pg_notify so the Python listener wakes up.
        #
        # We send only { id, event_type } in the notify payload rather than
        # the full row.  Reason: pg_notify has an 8 KB payload limit.
        # Sending a fat row risks silent truncation on wide tables; the
        # listener fetches the current record with a normal SELECT instead.
        cur.execute("""
            CREATE OR REPLACE FUNCTION notify_order_change()
            RETURNS TRIGGER AS $$
            DECLARE
                v_event_type TEXT;
                v_old_status TEXT;
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    v_event_type := 'insert';
                    v_old_status := NULL;
                ELSE
                    v_event_type := 'update';
                    v_old_status := OLD.status;
                END IF;

                -- 1. Audit trail
                INSERT INTO order_events (order_id, event_type, old_status, new_status)
                VALUES (NEW.id, v_event_type, v_old_status, NEW.status);

                -- 2. Wake up the Python listener
                PERFORM pg_notify(
                    'order_updates',
                    json_build_object('id', NEW.id, 'event_type', v_event_type)::text
                );

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)

        # ── trigger ──────────────────────────────────────────────────
        cur.execute("""
            DROP TRIGGER IF EXISTS order_change_trigger ON orders;
            CREATE TRIGGER order_change_trigger
            AFTER INSERT OR UPDATE ON orders
            FOR EACH ROW
            EXECUTE FUNCTION notify_order_change();
        """)

        conn.commit()
        cur.close()
        print("Database setup complete.")

    finally:
        conn.close()