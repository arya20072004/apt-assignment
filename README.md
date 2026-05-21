# APT Real-Time Order Update System

**Internship Assignment — Atypical Technologies Pvt. Ltd.**

A backend service that streams PostgreSQL row changes to connected browser clients in real time using `LISTEN/NOTIFY`, Flask-SocketIO, and zero client-side polling.

---

## Architecture

```
PostgreSQL 15 (Docker)
    └── AFTER INSERT/UPDATE trigger on orders
         └── pg_notify('order_updates', '{"id": 1, "event_type": "insert"}')
              └── psycopg2 LISTEN loop (background daemon thread)
                   └── _fetch_order() — SELECT current row by id
                        └── socketio.emit('order_update', order)
                             └── Browser clients (WebSocket)
```

The notify payload carries only `id` and `event_type`. The listener immediately fetches the full row with a normal `SELECT` rather than trusting the payload. This keeps the payload well under PostgreSQL's 8 KB `pg_notify` limit and ensures clients always receive committed, up-to-date data even if multiple updates fire in quick succession.

---

## Project Structure

```
apt-realtime/
├── app.py              # Flask app — REST endpoints, SocketIO handlers, startup
├── database.py         # DB config, schema setup, trigger function
├── listener.py         # Background thread — LISTEN loop → socketio.emit
├── templates/
│   └── index.html      # Live dashboard (vanilla JS + Socket.IO client)
├── requirements.txt
└── README.md
```

---

## Prerequisites

| Tool   | Version           |
|--------|-------------------|
| Python | 3.10+             |
| Docker | Any recent        |
| pip    | Bundled with Python |

---

## Setup & Run

### 1. Start PostgreSQL in Docker

```bash
docker run \
  --name apt-postgres \
  -e POSTGRES_USER=aptuser \
  -e POSTGRES_PASSWORD=aptpassword \
  -e POSTGRES_DB=aptdb \
  -p 5432:5432 \
  -d postgres:15
```

Verify it is running:

```bash
docker ps | grep apt-postgres
```

### 2. Create and activate a virtual environment

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# macOS / Linux
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the application

```bash
python app.py
```

Expected output:

```
2026-05-20 09:00:00 [INFO] __main__: Starting Flask-SocketIO on http://0.0.0.0:5000
Database setup complete.
Listening for database changes...
```

### 5. Open the dashboard

Navigate to **http://localhost:5000** in your browser.

---

## API Reference

| Method | Endpoint                     | Description                                      |
|--------|------------------------------|--------------------------------------------------|
| GET    | `/`                          | Serve the live dashboard                         |
| GET    | `/orders`                    | All orders, sorted by `updated_at DESC`          |
| GET    | `/orders/<id>`               | Single order by id — 404 if not found            |
| GET    | `/orders/<id>/history`       | Full audit trail for an order — 404 if not found |
| POST   | `/demo/insert`               | Insert a random order (triggers notify chain)    |
| POST   | `/demo/update`               | Update a random order's status (triggers notify chain) |

### Example responses

**`GET /orders/1`**
```json
{
  "id": 1,
  "customer_name": "Priya Sharma",
  "product_name": "Algo Strategy A",
  "status": "shipped",
  "created_at": "2026-05-20 09:00:00.000000",
  "updated_at": "2026-05-20 09:05:00.000000"
}
```

**`GET /orders/1/history`**
```json
[
  {
    "event_type": "insert",
    "old_status": null,
    "new_status": "pending",
    "occurred_at": "2026-05-20 09:00:00.000000"
  },
  {
    "event_type": "update",
    "old_status": "pending",
    "new_status": "shipped",
    "occurred_at": "2026-05-20 09:05:00.000000"
  }
]
```

---

## How It Works

### 1. Database layer (`database.py`)

`setup_database()` runs on every startup and is fully idempotent — every statement uses `IF NOT EXISTS` or `CREATE OR REPLACE`.

**Schema:**

`orders` — the main table. Has both `created_at` (set once on `INSERT`, never changed) and `updated_at` (refreshed on every `UPDATE`). Separating the two lets you calculate how long an order spent in each status.

`order_events` — an append-only audit log. The trigger writes one row here for every `INSERT` or `UPDATE` on `orders`, recording the old and new status. Rows are never modified or deleted. An index on `order_id` ensures history lookups are index seeks rather than sequential scans — PostgreSQL does not create indexes for foreign keys automatically.

**Trigger function `notify_order_change()`:**

Runs inside the same transaction as the `INSERT`/`UPDATE` that caused it. Does two things atomically:
1. Writes to `order_events`.
2. Calls `pg_notify('order_updates', '{"id": ..., "event_type": "..."}')`.

Because this runs inside the transaction, the notify is only delivered to listeners after the transaction commits — the listener can never receive a notification for a row that was rolled back.

### 2. Listener thread (`listener.py`)

Runs as a `daemon=True` background thread so it shuts down automatically with the main process.

**Why a dedicated connection with `AUTOCOMMIT`?**
`LISTEN` cannot run inside a transaction block. psycopg2's default isolation level wraps every statement in a transaction, which silently prevents notifications from being delivered. The listener connection is set to `ISOLATION_LEVEL_AUTOCOMMIT` to work around this.

**Poll loop design:**
The loop calls `conn.poll()` to drain the socket buffer, then processes every queued notification before sleeping. This ensures a burst of rapid inserts is fully processed without artificial latency between notifications. The thread only sleeps when the queue is empty.

**Fetch-after-notify:**
Rather than embedding the full row in the `pg_notify` payload (which risks hitting the 8 KB limit), the listener sends only `id` and `event_type`. `_handle_notify()` then fetches the current row with a `SELECT` using the listener's existing open connection — no new connection overhead per event.

**Automatic reconnect:**
If PostgreSQL restarts or the connection drops, `psycopg2.OperationalError` is caught, the dead connection is closed, and the thread sleeps for 5 seconds before reconnecting. The thread never exits on its own.

### 3. Flask application (`app.py`)

**`use_reloader=False`** — Flask's stat-reloader forks a second worker process on startup. Without this flag, both processes start a listener thread, every `pg_notify` gets processed twice, and every client receives two `order_update` events per change.

**`_row_to_order()` helper** — single function that maps a DB row tuple to a dict. Used by both `get_orders` and `get_order` so the serialisation format is defined in one place.

**`demo_update` query design** — uses `TABLESAMPLE BERNOULLI(50)` to pick a random order id rather than `ORDER BY RANDOM()`. `TABLESAMPLE` reads a random sample of pages; `ORDER BY RANDOM()` must sort every row. The fallback `SELECT id FROM orders LIMIT 1` handles the edge case where a very small table returns zero rows from the sample.

---

## Key Design Decisions

**Lean `pg_notify` payload**
Only `id` and `event_type` are sent in the notify. The listener fetches the full row via a `SELECT`. Avoids the 8 KB payload limit and ensures the client always gets the latest committed state.

**Audit log in the database, not the application**
`order_events` is written by a trigger inside the same transaction as the change. If the application crashes mid-flight or a transaction rolls back, the audit log is always consistent with the orders table — an application-level audit log (e.g. writing to a log file after the query) cannot guarantee this.

**Daemon thread for the listener**
`daemon=True` means Python kills the thread automatically when the main process exits. No shutdown hooks, no `threading.Event`, no cleanup code needed for a dev/assignment context.

**Cursor reuse in the listen loop**
The listener opens one cursor for the duration of its connection and passes it into `_fetch_order`. Opening a new connection per notification would work but wastes a Postgres backend slot and adds TCP + auth overhead on every event.

---

## Known Limitations

These are intentional trade-offs for an assignment scope, and worth being able to discuss:

- **No connection pool** — each HTTP request opens and closes its own `psycopg2` connection. Under real load this would be replaced with `psycopg2.pool.ThreadedConnectionPool` or SQLAlchemy's connection pool.
- **Single listener thread** — one background thread handles all notifications. A high-throughput system would use multiple workers or an async approach (e.g. `asyncpg` with `asyncio`).
- **`ORDER BY RANDOM()` fallback in `demo_update`** — the `TABLESAMPLE` fallback path still uses `LIMIT 1` without ordering, which returns an arbitrary row. Acceptable for a demo; not for production.
- **No authentication** — the REST API and WebSocket are open. A real deployment would add token validation on the SocketIO `connect` handler and auth middleware on the REST routes.

---

## Stopping the Server

Press `Ctrl+C` in the terminal running `app.py`.

Stop and remove the Docker container:

```bash
docker stop apt-postgres && docker rm apt-postgres
```