import sqlite3
from rapidfuzz import fuzz
from backend.database import get_conn


def match_shipments():
    """
    Match export invoices to UPS invoices using tracking ID and fuzzy logic.
    Creates or updates shipment records.
    """
    conn = get_conn()
    c = conn.cursor()

    # Get all unmatched export invoices
    c.execute('''
        SELECT ei.*, d.filename FROM export_invoices ei
        LEFT JOIN documents d ON ei.document_id = d.id
        WHERE ei.tracking_id IS NOT NULL AND ei.tracking_id != ''
    ''')
    export_invs = c.fetchall()

    # Get all UPS invoices
    c.execute('''
        SELECT ui.*, d.filename FROM ups_invoices ui
        LEFT JOIN documents d ON ui.document_id = d.id
        WHERE ui.tracking_number IS NOT NULL AND ui.tracking_number != ''
    ''')
    ups_invs = c.fetchall()

    matched_count = 0

    for ei in export_invs:
        best_ups = None
        best_score = 0

        for ui in ups_invs:
            score = 0

            # Exact tracking ID match (highest weight)
            if ei['tracking_id'] and ui['tracking_number']:
                t1 = ei['tracking_id'].upper().strip()
                t2 = ui['tracking_number'].upper().strip()
                if t1 == t2:
                    score = 100
                else:
                    score = fuzz.ratio(t1, t2)

            if score > best_score:
                best_score = score
                best_ups = ui

        # Use match if score >= 80
        ups_id = best_ups['id'] if best_ups and best_score >= 80 else None

        # Calculate totals
        total_cost = 0.0
        cost_per_kg = 0.0
        if best_ups and best_score >= 80:
            total_cost = best_ups['total_charge'] or 0.0
            weight = ei['gross_weight'] or best_ups['billed_weight'] or 1
            if weight > 0:
                cost_per_kg = round(total_cost / weight, 2)

        # Determine status
        status = 'matched' if ups_id else 'unmatched'

        # Upsert shipment record
        existing = c.execute(
            'SELECT id FROM shipments WHERE tracking_id = ?', (ei['tracking_id'],)
        ).fetchone()

        if existing:
            c.execute('''
                UPDATE shipments SET
                    export_invoice_id = ?,
                    ups_invoice_id = ?,
                    ship_date = ?,
                    destination = ?,
                    consignee = ?,
                    gross_weight = ?,
                    total_cost = ?,
                    cost_per_kg = ?,
                    status = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE tracking_id = ?
            ''', (
                ei['id'], ups_id, ei['invoice_date'],
                ei['destination_country'], ei['consignee'],
                ei['gross_weight'], total_cost, cost_per_kg, status,
                ei['tracking_id']
            ))
        else:
            c.execute('''
                INSERT INTO shipments
                    (tracking_id, export_invoice_id, ups_invoice_id, ship_date,
                     destination, consignee, gross_weight, total_cost, cost_per_kg, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ei['tracking_id'], ei['id'], ups_id, ei['invoice_date'],
                ei['destination_country'], ei['consignee'],
                ei['gross_weight'], total_cost, cost_per_kg, status
            ))
            matched_count += 1

    conn.commit()
    conn.close()
    return {"matched": matched_count, "total_export": len(export_invs), "total_ups": len(ups_invs)}


def get_unmatched_shipments():
    """Return shipments with no UPS invoice match."""
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute(
        "SELECT * FROM shipments WHERE ups_invoice_id IS NULL"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
