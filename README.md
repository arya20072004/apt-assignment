# APT Real-Time Order Update System

**Internship Assignment — Atypical Technologies Pvt. Ltd.**

A real-time order tracking dashboard that streams database changes directly to connected browser clients using PostgreSQL `NOTIFY`, Flask-SocketIO WebSockets, and zero client-side polling.

---

## Architecture

```
PostgreSQL 15 (Docker)
    └─ Trigger fires on INSERT / UPDATE to orders table
         └─ pg_notify('order_updates', row_to_json(NEW))
              └─ Flask backend (psycopg2 LISTEN loop)
                   └─ socketio.emit('order_update', payload)
                        └─ Browser client (live dashboard)
```

Every row change in PostgreSQL travels through this chain in under a second — no polling, no cron jobs, no REST hammering.

---

## Project Structure

```
apt-realtime/
├── app.py            # Flask app, REST endpoints, SocketIO handlers
├── database.py       # DB config, connection helper, schema + trigger setup
├── listener.py       # Background thread: LISTEN → socketio.emit
├── templates/
│   └── index.html    # Dark-themed live dashboard (vanilla JS + Socket.IO)
├── requirements.txt
└── README.md
```

---

## Prerequisites

| Tool | Version |
|------|---------|
| Python | 3.10+ |
| Docker | Any recent version |
| pip | Bundled with Python |

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

Verify it's running:

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
Database setup complete.
Starting Flask server on http://localhost:5000
Listening for database changes...
```

### 5. Open the dashboard

Navigate to **http://localhost:5000** in your browser.

---

## How It Works

### PostgreSQL Side

`setup_database()` in `database.py` creates:

- **`orders` table** — stores `id`, `customer_name`, `product_name`, `status`, `updated_at`
- **`notify_order_change()` function** — calls `pg_notify('order_updates', row_to_json(NEW)::text)` to publish the changed row as a JSON string
- **`order_change_trigger`** — fires `AFTER INSERT OR UPDATE` on the `orders` table, calling the function above for every affected row

### Python Listener

`start_listener()` in `listener.py` runs in a background daemon thread:

1. Opens a dedicated psycopg2 connection with `AUTOCOMMIT` isolation (required — `LISTEN` cannot run inside a transaction)
2. Executes `LISTEN order_updates;`
3. Enters a loop calling `conn.poll()` every 500 ms to drain any pending notifications
4. For each notification, parses the JSON payload and calls `socketio.emit('order_update', payload)` to broadcast to all connected clients

> **Why `conn.poll()` instead of `select.select()`?**  
> `select.select()` on a psycopg2 connection uses the raw file descriptor, which is not available on Windows. `conn.poll()` is the portable, officially supported approach.

### Flask / SocketIO

`app.py` wires everything together:

- `GET /` — serves the dashboard
- `GET /orders` — returns all current orders as JSON (used for initial page load)
- `POST /demo/insert` — inserts a random order (triggers the notify chain)
- `POST /demo/update` — updates a random order's status (triggers the notify chain)
- `use_reloader=False` — prevents Flask's stat-reloader from forking a second process, which would otherwise spawn two listener threads and produce duplicate notifications

### Browser Client

`index.html` connects via Socket.IO and:

1. Fetches existing orders from `/orders` on load and renders them
2. Listens for `order_update` events and upserts the row in the table (insert if new ID, update in-place if existing)
3. Flashes the affected row with a green highlight animation
4. Appends a timestamped entry to the event log panel

---

## Demo

With the server running, click the buttons in the dashboard:

| Button | Action | What you see |
|--------|--------|--------------|
| **+ Insert Random Order** | POSTs to `/demo/insert` | New row appears instantly, flashes green |
| **↺ Update Random Status** | POSTs to `/demo/update` | Existing row's badge changes, flashes green |
| **Clear Log** | Client-side only | Clears the event log panel |

Open the dashboard in two browser tabs simultaneously — updates triggered in one tab appear in both in real time, demonstrating the broadcast nature of SocketIO.

---

## Key Design Decisions

**PostgreSQL LISTEN/NOTIFY over polling**  
The database itself pushes change events. The backend never queries `SELECT` in a loop to detect changes — the notify arrives within milliseconds of the commit.

**Daemon thread for the listener**  
Running the listener in a `daemon=True` thread means it automatically shuts down when the main Flask process exits, without needing explicit cleanup code.

**Upsert on the client**  
The frontend uses the order `id` as a row key. An incoming `order_update` event either inserts a new `<tr>` or updates an existing one, so the table always reflects the true database state without a full reload.

---

## Stopping the Server

Press `Ctrl+C` in the terminal running `app.py`.

To stop and remove the Docker container:

```bash
docker stop apt-postgres
docker rm apt-postgres
```