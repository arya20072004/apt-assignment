import logging
import os
import random
import threading

from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO

from database import setup_database, get_connection
from listener import start_listener

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# SECRET_KEY must come from the environment in any real deployment.
# It is used by Flask to sign session cookies — a hardcoded value means
# anyone who reads the source can forge sessions.
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "apt-dev-secret-change-in-prod")

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

_CUSTOMERS = ["Ravi Kumar", "Priya Sharma", "Amit Patel", "Sneha Joshi", "Arjun Singh"]
_PRODUCTS  = ["Algo Strategy A", "Options Pack", "Nifty Bundle", "Index Tracker", "Futures Kit"]
_STATUSES  = ["pending", "shipped", "delivered"]


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


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    """
    Liveness check — verifies the DB connection is reachable.
    In production this would be polled by a load balancer or k8s probe.
    """
    try:
        conn = get_connection()
        conn.close()
        return jsonify({"status": "ok", "database": "reachable"}), 200
    except Exception as e:
        return jsonify({"status": "error", "database": str(e)}), 503


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
    """Return a single order by id. 404 if not found."""
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
    Return the full audit trail for an order from order_events.
    Sorted oldest-first so callers can reconstruct the state transitions.
    404 if no events exist for the given id.
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

    return jsonify([
        {
            "event_type":  row[0],
            "old_status":  row[1],
            "new_status":  row[2],
            "occurred_at": str(row[3]),
        }
        for row in rows
    ])


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

    Uses TABLESAMPLE BERNOULLI to pick a random id without a full-table
    sort — much cheaper than ORDER BY RANDOM() at scale.  Falls back to
    a plain LIMIT 1 if the sample returns zero rows on a small table.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("SELECT id FROM orders TABLESAMPLE BERNOULLI(50) LIMIT 1;")
        row = cur.fetchone()

        if row is None:
            cur.execute("SELECT id FROM orders LIMIT 1;")
            row = cur.fetchone()

        if row is None:
            conn.rollback()
            cur.close()
            return jsonify({"error": "No orders available to update"}), 404

        cur.execute("""
            UPDATE orders
            SET    status     = %s,
                   updated_at = CURRENT_TIMESTAMP
            WHERE  id = %s
            RETURNING id, status;
        """, (random.choice(_STATUSES), row[0]))

        updated = cur.fetchone()
        conn.commit()
        cur.close()
    finally:
        conn.close()

    log.info("Demo update: order #%s → %s", updated[0], updated[1])
    return jsonify({"message": "Order updated", "id": updated[0], "status": updated[1]}), 200


@socketio.on("connect")
def handle_connect():
    log.info("WebSocket client connected (sid=%s)", request_sid())

@socketio.on("disconnect")
def handle_disconnect():
    log.info("WebSocket client disconnected (sid=%s)", request_sid())

def request_sid():
    try:
        from flask import request
        return request.sid
    except Exception:
        return "?"


if __name__ == "__main__":
    setup_database()

    listener_thread = threading.Thread(
        target=start_listener,
        args=(socketio,),
        daemon=True,
        name="pg-listener",
    )
    listener_thread.start()

    log.info("Starting Flask-SocketIO on http://0.0.0.0:5000")
    socketio.run(app, debug=True, use_reloader=False, host="0.0.0.0", port=5000)