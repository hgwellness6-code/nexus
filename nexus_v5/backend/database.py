import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'nexus.db')

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_conn()
    c = conn.cursor()

    c.executescript('''
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        filepath TEXT NOT NULL,
        doc_type TEXT,
        raw_text TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS export_invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
        invoice_number TEXT,
        invoice_date TEXT,
        consignee TEXT,
        destination_country TEXT,
        tracking_id TEXT,
        gross_weight REAL,
        chargeable_weight REAL,
        declared_value REAL,
        currency TEXT DEFAULT 'USD',
        product_desc TEXT,
        confidence REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS ups_invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
        ups_invoice_number TEXT,
        invoice_date TEXT,
        tracking_number TEXT,
        service_type TEXT,
        billed_weight REAL,
        transport_charge REAL DEFAULT 0,
        fuel_surcharge REAL DEFAULT 0,
        remote_area_surcharge REAL DEFAULT 0,
        duty_tax REAL DEFAULT 0,
        other_charges REAL DEFAULT 0,
        total_charge REAL DEFAULT 0,
        currency TEXT DEFAULT 'USD',
        confidence REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS shipments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tracking_id TEXT UNIQUE NOT NULL,
        export_invoice_id INTEGER REFERENCES export_invoices(id),
        ups_invoice_id INTEGER REFERENCES ups_invoices(id),
        ship_date TEXT,
        destination TEXT,
        consignee TEXT,
        gross_weight REAL,
        total_cost REAL DEFAULT 0,
        cost_per_kg REAL DEFAULT 0,
        status TEXT DEFAULT 'pending',
        folder_path TEXT,
        notes TEXT,
        tags TEXT DEFAULT '',
        priority TEXT DEFAULT 'normal',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS reminders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shipment_id INTEGER REFERENCES shipments(id),
        reminder_type TEXT,
        due_date TEXT,
        days_after_ship INTEGER,
        email_to TEXT,
        email_from TEXT,
        message_template TEXT,
        is_sent INTEGER DEFAULT 0,
        is_enabled INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS reminder_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        days_after INTEGER NOT NULL,
        reminder_type TEXT NOT NULL,
        subject_template TEXT,
        body_template TEXT,
        is_enabled INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        action TEXT,
        entity_type TEXT,
        entity_id INTEGER,
        details TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS saved_searches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        query TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_shipments_tracking ON shipments(tracking_id);
    CREATE INDEX IF NOT EXISTS idx_shipments_status ON shipments(status);
    CREATE INDEX IF NOT EXISTS idx_shipments_destination ON shipments(destination);
    CREATE INDEX IF NOT EXISTS idx_shipments_ship_date ON shipments(ship_date);
    CREATE INDEX IF NOT EXISTS idx_export_tracking ON export_invoices(tracking_id);
    CREATE INDEX IF NOT EXISTS idx_ups_tracking ON ups_invoices(tracking_number);

    INSERT OR IGNORE INTO reminder_templates (name, days_after, reminder_type, subject_template, body_template, is_enabled)
    VALUES
        ('POD Reminder', 7, 'pod', 'POD Required: Shipment {tracking_id}', 'Please provide Proof of Delivery for shipment {tracking_id} shipped on {ship_date} to {destination}.', 1),
        ('Customs Clearance Docs', 10, 'customs', 'Customs Docs Required: {tracking_id}', 'Please provide customs clearance documents for shipment {tracking_id}.', 1),
        ('Final Audit', 15, 'audit', 'Final Audit: Shipment {tracking_id}', 'Please complete final audit for shipment {tracking_id}. All docs should be received by now.', 0),
        ('Duty Refund Follow-up', 30, 'duty', 'Duty Refund Check: {tracking_id}', 'Check duty drawback eligibility for shipment {tracking_id}.', 0);
    ''')

    conn.commit()
    conn.close()
    print(f"[DB] Initialized at {DB_PATH}")
