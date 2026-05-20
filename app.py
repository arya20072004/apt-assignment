from flask import Flask, render_template
from flask_socketio import SocketIO
import threading
from database import setup_database, get_connection
from listener import start_listener
import json
import random

app = Flask(__name__)
app.config["SECRET_KEY"] = "apt-secret-key"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

@app.route("/")
def index():
    """Serve the main dashboard page."""
    return render_template("index.html")

@app.route("/orders")
def get_orders():
    """REST endpoint to fetch all current orders."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, customer_name, product_name, status, updated_at FROM orders ORDER BY updated_at DESC;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    orders = [
        {
            "id": row[0],
            "customer_name": row[1],
            "product_name": row[2],
            "status": row[3],
            "updated_at": str(row[4])
        }
        for row in rows
    ]
    return json.dumps(orders)

@socketio.on("connect")
def handle_connect():
    print("Client connected via WebSocket")

@socketio.on("disconnect")
def handle_disconnect():
    print("Client disconnected")

@app.route("/demo/insert", methods=["POST"])
def demo_insert():
    """Insert a random order for demo purposes."""
    customers = ["Ravi Kumar", "Priya Sharma", "Amit Patel", "Sneha Joshi", "Arjun Singh"]
    products  = ["Algo Strategy A", "Options Pack", "Nifty Bundle", "Index Tracker", "Futures Kit"]

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO orders (customer_name, product_name, status, updated_at)
        VALUES (%s, %s, 'pending', CURRENT_TIMESTAMP)
    """, (random.choice(customers), random.choice(products)))
    conn.commit()
    cur.close()
    conn.close()
    return {"message": "Order inserted"}, 201


@app.route("/demo/update", methods=["POST"])
def demo_update():
    """Update a random existing order's status."""
    statuses = ["pending", "shipped", "delivered"]

    conn = get_connection()
    cur = conn.cursor()

    # Pick a random existing order
    cur.execute("SELECT id FROM orders ORDER BY RANDOM() LIMIT 1;")
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return {"message": "No orders to update"}, 404

    cur.execute("""
        UPDATE orders
        SET status = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (random.choice(statuses), row[0]))

    conn.commit()
    cur.close()
    conn.close()
    return {"message": "Order updated"}, 200

if __name__ == "__main__":
    # Setup database, tables and triggers
    setup_database()

    # Start PostgreSQL listener in background thread
    listener_thread = threading.Thread(
        target=start_listener,
        args=(socketio,),
        daemon=True
    )
    listener_thread.start()

    # Start Flask app
    print("Starting Flask server on http://localhost:5000")
    socketio.run(app, debug=True, host="0.0.0.0", port=5000)