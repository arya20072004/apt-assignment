import psycopg2
import select
import json
from database import get_connection

def start_listener(socketio):
    """
    Listens for PostgreSQL NOTIFY events on 'order_updates' channel
    and emits them to all connected WebSocket clients.
    """
    conn = get_connection()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Start listening on the channel
    cur.execute("LISTEN order_updates;")
    print("Listening for database changes...")

    while True:
        # Wait for notification (timeout 5 seconds, then check again)
        if select.select([conn], [], [], 5) == ([], [], []):
            continue

        conn.poll()

        while conn.notifies:
            notify = conn.notifies.pop(0)
            payload = json.loads(notify.payload)

            print(f"Change detected: {payload}")

            # Push update to all connected browser clients
            socketio.emit("order_update", payload)