import smtplib
import sqlite3
import json
import os
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from backend.database import get_conn

SETTINGS_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'settings.json')


def load_settings():
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE) as f:
            return json.load(f)
    return {
        "email_from": "",
        "email_password": "",
        "email_to": "",
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 587,
    }


def save_settings(settings: dict):
    os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(settings, f, indent=2)


def create_reminders_for_shipment(shipment_id: int):
    """Create reminder records for a shipment based on templates."""
    conn = get_conn()
    c = conn.cursor()

    shipment = c.execute(
        'SELECT * FROM shipments WHERE id = ?', (shipment_id,)
    ).fetchone()
    if not shipment:
        conn.close()
        return []

    templates = c.execute(
        'SELECT * FROM reminder_templates WHERE is_enabled = 1'
    ).fetchall()

    settings = load_settings()
    created = []

    for tmpl in templates:
        try:
            if shipment['ship_date']:
                ship_dt = datetime.strptime(shipment['ship_date'][:10], '%Y-%m-%d')
            else:
                ship_dt = datetime.now()
            due_dt = ship_dt + timedelta(days=tmpl['days_after'])
        except Exception:
            due_dt = datetime.now() + timedelta(days=tmpl['days_after'])

        tracking_id = shipment['tracking_id'] or ''
        subject = (tmpl['subject_template'] or '').replace('{tracking_id}', tracking_id)
        body = (tmpl['body_template'] or '').replace('{tracking_id}', tracking_id)\
                                             .replace('{ship_date}', shipment['ship_date'] or '')\
                                             .replace('{destination}', shipment['destination'] or '')

        existing = c.execute('''
            SELECT id FROM reminders
            WHERE shipment_id = ? AND reminder_type = ?
        ''', (shipment_id, tmpl['reminder_type'])).fetchone()

        if not existing:
            c.execute('''
                INSERT INTO reminders
                    (shipment_id, reminder_type, due_date, days_after_ship,
                     email_to, email_from, message_template, is_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            ''', (
                shipment_id, tmpl['reminder_type'],
                due_dt.strftime('%Y-%m-%d'), tmpl['days_after'],
                settings.get('email_to', ''), settings.get('email_from', ''),
                json.dumps({"subject": subject, "body": body})
            ))
            created.append(tmpl['reminder_type'])

    conn.commit()
    conn.close()
    return created


def get_due_reminders():
    """Return all reminders due today or overdue."""
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    rows = c.execute('''
        SELECT r.*, s.tracking_id, s.destination, s.ship_date
        FROM reminders r
        JOIN shipments s ON r.shipment_id = s.id
        WHERE r.is_sent = 0 AND r.is_enabled = 1 AND r.due_date <= ?
        ORDER BY r.due_date ASC
    ''', (today,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_upcoming_reminders(days_ahead: int = 30):
    """Return upcoming reminders in the next N days."""
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    future = (datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
    rows = c.execute('''
        SELECT r.*, s.tracking_id, s.destination, s.ship_date, s.total_cost
        FROM reminders r
        JOIN shipments s ON r.shipment_id = s.id
        WHERE r.is_sent = 0 AND r.is_enabled = 1
          AND r.due_date BETWEEN ? AND ?
        ORDER BY r.due_date ASC
    ''', (today, future)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def send_reminder_email(reminder_id: int) -> dict:
    """Send a reminder email using SMTP (Gmail or other)."""
    settings = load_settings()

    if not settings.get('email_from') or not settings.get('email_password'):
        return {"success": False, "error": "Email not configured. Add SMTP settings in Settings."}

    conn = get_conn()
    c = conn.cursor()
    rem = c.execute(
        'SELECT * FROM reminders WHERE id = ?', (reminder_id,)
    ).fetchone()
    if not rem:
        conn.close()
        return {"success": False, "error": "Reminder not found"}

    try:
        tmpl = json.loads(rem['message_template'] or '{}')
    except Exception:
        tmpl = {"subject": "Shipment Reminder", "body": "Please check this shipment."}

    try:
        msg = MIMEMultipart()
        msg['From'] = settings['email_from']
        msg['To'] = rem['email_to'] or settings['email_to']
        msg['Subject'] = tmpl.get('subject', 'Shipment Reminder')
        msg.attach(MIMEText(tmpl.get('body', ''), 'plain'))

        with smtplib.SMTP(settings.get('smtp_host', 'smtp.gmail.com'),
                          settings.get('smtp_port', 587)) as server:
            server.starttls()
            server.login(settings['email_from'], settings['email_password'])
            server.send_message(msg)

        c.execute(
            'UPDATE reminders SET is_sent = 1 WHERE id = ?', (reminder_id,)
        )
        conn.commit()
        conn.close()
        return {"success": True, "message": f"Email sent to {msg['To']}"}

    except Exception as e:
        conn.close()
        return {"success": False, "error": str(e)}


def mark_reminder_sent(reminder_id: int):
    conn = get_conn()
    conn.execute('UPDATE reminders SET is_sent = 1 WHERE id = ?', (reminder_id,))
    conn.commit()
    conn.close()
