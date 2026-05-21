import logging
import os
import random
import threading

from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO

from database import setup_database, get_connection
from listener import start_listener

# ── Logging ───────────────────────────────────────────────────────────────────
# Configured once here so every module that calls logging.getLogger(__name__)
# inherits the same format automatically.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── App setup ─────────────────────────────────────────────────────────────────
app = Flask(__name__)

# SECRET_KEY must come from the environment in any real deployment.
# It is used by Flask to sign session cookies — a hardcoded value means
# anyone who reads the source can forge sessions.
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "apt-dev-secret-change-in-prod")

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# ── Demo seed data ────────────────────────────────────────────────────────────
_CUSTOMERS = ["Ravi Kumar", "Priya Sharma", "Amit Patel", "Sneha Joshi", "Arjun Singh"]
_PRODUCTS  = ["Algo Strategy A", "Options Pack", "Nifty Bundle", "Index Tracker", "Futures Kit"]
_STATUSES  = ["pending", "shipped", "delivered"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_order(row) -> dict:
    """Map a DB row tuple from the orders table to a JSON-serialisable dict."""
    return {
        "id":            row[0],
        "customer_name": row[1],
        "product_name":  row[2],
        "status":        row[3],
        "created_at":    str(row[4]),
        "updated_at":    str(row[5]),
    }


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Serve the main dashboard page."""
    return render_template("index.html")


@app.route("/orders")
def get_orders():
    """
    Return all orders sorted by most-recently updated.
    Called by the dashboard on initial page load to populate the table
    before the WebSocket connection is established.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, customer_name, product_name, status, created_at, updated_at
            FROM   orders
            ORDER  BY updated_at DESC;
        """)
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    return jsonify([_row_to_order(r) for r in rows])


@app.route("/orders/<int:order_id>")
def get_order(order_id):
    """
    Return a single order by id.
    Returns 404 if the order does not exist.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, customer_name, product_name, status, created_at, updated_at
            FROM   orders
            WHERE  id = %s;
        """, (order_id,))
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if row is None:
        return jsonify({"error": f"Order {order_id} not found"}), 404

    return jsonify(_row_to_order(row))


@app.route("/orders/<int:order_id>/history")
def get_order_history(order_id):
    """
    Return the full audit trail for a single order from order_events.
    Events are sorted oldest-first so callers can reconstruct the
    state machine in order.
    Returns 404 if no events exist for the given order id — which means
    the order itself does not exist.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT event_type, old_status, new_status, occurred_at
            FROM   order_events
            WHERE  order_id = %s
            ORDER  BY occurred_at ASC;
        """, (order_id,))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    if not rows:
        return jsonify({"error": f"No history found for order {order_id}"}), 404

    history = [
        {
            "event_type":  row[0],
            "old_status":  row[1],
            "new_status":  row[2],
            "occurred_at": str(row[3]),
        }
        for row in rows
    ]
    return jsonify(history)


# ── Demo endpoints ────────────────────────────────────────────────────────────

@app.route("/demo/insert", methods=["POST"])
def demo_insert():
    """
    Insert a random order with status 'pending'.
    The DB trigger fires automatically and pushes the update to all
    connected WebSocket clients via pg_notify → listener → socketio.emit.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO orders (customer_name, product_name, status)
            VALUES (%s, %s, 'pending')
            RETURNING id;
        """, (random.choice(_CUSTOMERS), random.choice(_PRODUCTS)))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
    finally:
        conn.close()

    log.info("Demo insert: created order #%s", new_id)
    return jsonify({"message": "Order inserted", "id": new_id}), 201


@app.route("/demo/update", methods=["POST"])
def demo_update():
    """
    Set a random existing order to a random status.

    Query design: SELECT a random id first using TABLESAMPLE BERNOULLI,
    then UPDATE only that specific row by primary key.  This avoids
    locking the whole table during the UPDATE and keeps the write path
    to a single-row operation regardless of table size.

    TABLESAMPLE BERNOULLI(p) reads a random p% of pages — much cheaper
    than ORDER BY RANDOM() which must sort every row.  LIMIT 1 + OFFSET
    handles the edge case where the sample returns zero rows by falling
    back to a guaranteed single-row fetch.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Sample a random id without a full-table sort.
        cur.execute("""
            SELECT id FROM orders
            TABLESAMPLE BERNOULLI(50)
            LIMIT 1;
        """)
        row = cur.fetchone()

        # TABLESAMPLE can return 0 rows on a very small table — fall back
        # to a direct fetch in that case.
        if row is None:
            cur.execute("SELECT id FROM orders LIMIT 1;")
            row = cur.fetchone()

        if row is None:
            conn.rollback()
            cur.close()
            return jsonify({"error": "No orders available to update"}), 404

        target_id    = row[0]
        new_status   = random.choice(_STATUSES)

        cur.execute("""
            UPDATE orders
            SET    status     = %s,
                   updated_at = CURRENT_TIMESTAMP
            WHERE  id = %s
            RETURNING id, status;
        """, (new_status, target_id))

        updated = cur.fetchone()
        conn.commit()
        cur.close()
    finally:
        conn.close()

    log.info("Demo update: order #%s → %s", updated[0], updated[1])
    return jsonify({"message": "Order updated", "id": updated[0], "status": updated[1]}), 200


# ── WebSocket handlers ────────────────────────────────────────────────────────

@socketio.on("connect")
def handle_connect():
    log.info("WebSocket client connected (sid=%s)", request_sid())

@socketio.on("disconnect")
def handle_disconnect():
    log.info("WebSocket client disconnected (sid=%s)", request_sid())

def request_sid():
    """Return the current Socket.IO session id, or '?' if unavailable."""
    try:
        from flask import request
        return request.sid
    except Exception:
        return "?"


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_database()

    # Run the PostgreSQL LISTEN loop in a background daemon thread.
    # daemon=True means this thread is killed automatically when the main
    # process exits — no manual cleanup needed.
    #
    # use_reloader=False is critical: Flask's stat-reloader forks a second
    # worker process on startup, which would launch a second listener thread
    # and cause every pg_notify to be processed and emitted twice.
    listener_thread = threading.Thread(
        target=start_listener,
        args=(socketio,),
        daemon=True,
        name="pg-listener",
    )
    listener_thread.start()

    log.info("Starting Flask-SocketIO on http://0.0.0.0:5000")
    socketio.run(app, debug=True, use_reloader=False, host="0.0.0.0", port=5000)