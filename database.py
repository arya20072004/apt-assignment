import psycopg2

DB_CONFIG = {
    "dbname": "aptdb",
    "user": "aptuser",
    "password": "aptpassword",
    "host": "localhost",
    "port": 5432
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def setup_database():
    conn = get_connection()
    cur = conn.cursor()

    # Create orders table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            customer_name VARCHAR(100),
            product_name VARCHAR(100),
            status VARCHAR(20) CHECK (status IN ('pending', 'shipped', 'delivered')),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Create function that sends NOTIFY on any change
    cur.execute("""
        CREATE OR REPLACE FUNCTION notify_order_change()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify(
                'order_updates',
                row_to_json(NEW)::text
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # Create trigger that fires on INSERT, UPDATE, DELETE
    cur.execute("""
        DROP TRIGGER IF EXISTS order_change_trigger ON orders;
        CREATE TRIGGER order_change_trigger
        AFTER INSERT OR UPDATE ON orders
        FOR EACH ROW
        EXECUTE FUNCTION notify_order_change();
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Database setup complete.")