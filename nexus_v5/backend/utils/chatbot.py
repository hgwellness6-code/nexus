import re
import json
from backend.database import get_conn


def query_shipments(user_query: str) -> dict:
    """
    Parse natural language query and run against SQLite.
    Returns answer text + optional data rows.
    """
    q = user_query.lower().strip()
    conn = get_conn()
    c = conn.cursor()

    response = {"answer": "", "data": [], "type": "text"}

    # --- POD / missing docs ---
    if any(x in q for x in ['pod', 'proof of delivery', 'missing pod']):
        rows = c.execute('''
            SELECT s.tracking_id, s.destination, s.ship_date, s.total_cost,
                   julianday('now') - julianday(s.ship_date) as days_since
            FROM shipments s
            WHERE s.status NOT IN ('closed') AND s.folder_path IS NULL
               OR s.tracking_id NOT IN (
                   SELECT DISTINCT tracking_id FROM shipments
                   WHERE folder_path LIKE '%pod.pdf%'
               )
            ORDER BY s.ship_date DESC LIMIT 20
        ''').fetchall()
        # Simpler fallback: show all non-closed shipments
        rows2 = c.execute('''
            SELECT tracking_id, destination, ship_date, total_cost
            FROM shipments WHERE status != 'closed' ORDER BY ship_date DESC LIMIT 10
        ''').fetchall()
        data = [dict(r) for r in rows2]
        response["answer"] = f"Found {len(data)} shipments that may need POD follow-up:"
        response["data"] = data
        response["type"] = "shipment_list"

    # --- Missing docs / customs ---
    elif any(x in q for x in ['missing', 'customs', 'documents', 'unmatched']):
        rows = c.execute('''
            SELECT tracking_id, destination, ship_date, total_cost, status
            FROM shipments WHERE status = 'unmatched' OR ups_invoice_id IS NULL
            ORDER BY ship_date DESC LIMIT 10
        ''').fetchall()
        data = [dict(r) for r in rows]
        response["answer"] = f"Found {len(data)} shipments with missing UPS invoice or unmatched documents."
        response["data"] = data
        response["type"] = "shipment_list"

    # --- Cost above threshold ---
    elif re.search(r'above|more than|greater|expensive|over\s*\$?(\d+)', q):
        m = re.search(r'\$?(\d+)', q)
        threshold = float(m.group(1)) if m else 150.0
        rows = c.execute('''
            SELECT tracking_id, destination, ship_date, gross_weight, total_cost, cost_per_kg
            FROM shipments WHERE total_cost > ?
            ORDER BY total_cost DESC LIMIT 15
        ''', (threshold,)).fetchall()
        data = [dict(r) for r in rows]
        response["answer"] = f"Found {len(data)} shipments costing more than ${threshold:.0f}:"
        response["data"] = data
        response["type"] = "shipment_list"

    # --- Fuel surcharge ---
    elif any(x in q for x in ['fuel', 'surcharge']):
        rows = c.execute('''
            SELECT s.tracking_id, s.destination, ui.fuel_surcharge,
                   ui.total_charge, s.ship_date
            FROM shipments s
            JOIN ups_invoices ui ON s.ups_invoice_id = ui.id
            ORDER BY ui.fuel_surcharge DESC LIMIT 10
        ''').fetchall()
        data = [dict(r) for r in rows]
        if data:
            top = data[0]
            avg = sum(r['fuel_surcharge'] or 0 for r in data) / len(data)
            response["answer"] = (
                f"Highest fuel surcharge: ${top['fuel_surcharge']:.2f} on shipment "
                f"{top['tracking_id']} to {top['destination']}. "
                f"Average fuel surcharge across top 10: ${avg:.2f}."
            )
        else:
            response["answer"] = "No UPS invoice data found yet. Import UPS invoices to see surcharge data."
        response["data"] = data
        response["type"] = "charge_list"

    # --- Average cost per KG by country ---
    elif any(x in q for x in ['average', 'avg', 'per kg', 'by country', 'cost per']):
        rows = c.execute('''
            SELECT destination,
                   COUNT(*) as count,
                   ROUND(AVG(total_cost), 2) as avg_cost,
                   ROUND(AVG(cost_per_kg), 2) as avg_per_kg,
                   ROUND(SUM(total_cost), 2) as total
            FROM shipments
            WHERE total_cost > 0 AND destination IS NOT NULL
            GROUP BY destination
            ORDER BY avg_per_kg DESC
        ''').fetchall()
        data = [dict(r) for r in rows]
        if data:
            lines = [f"{r['destination']}: ${r['avg_per_kg']:.2f}/kg (avg)" for r in data[:6]]
            response["answer"] = "Average cost per KG by country:\n" + "\n".join(lines)
        else:
            response["answer"] = "No shipment data yet. Import invoices to see cost analysis."
        response["data"] = data
        response["type"] = "country_table"

    # --- Specific invoice lookup ---
    elif re.search(r'(inv|invoice|exp)[- ]?\d+', q, re.IGNORECASE):
        inv_m = re.search(r'([A-Z0-9\-]{5,20})', user_query, re.IGNORECASE)
        if inv_m:
            inv_no = inv_m.group(1).upper()
            rows = c.execute('''
                SELECT s.*, ei.invoice_number, ei.consignee,
                       ui.ups_invoice_number, ui.total_charge,
                       ui.transport_charge, ui.fuel_surcharge
                FROM shipments s
                LEFT JOIN export_invoices ei ON s.export_invoice_id = ei.id
                LEFT JOIN ups_invoices ui ON s.ups_invoice_id = ui.id
                WHERE ei.invoice_number LIKE ? OR s.tracking_id LIKE ?
                LIMIT 5
            ''', (f'%{inv_no}%', f'%{inv_no}%')).fetchall()
            data = [dict(r) for r in rows]
            if data:
                r = data[0]
                response["answer"] = (
                    f"Shipment {r.get('tracking_id', 'N/A')}: "
                    f"Export Invoice {r.get('invoice_number', 'N/A')}, "
                    f"UPS Invoice {r.get('ups_invoice_number', 'N/A')}. "
                    f"Total cost: ${r.get('total_cost', 0):.2f} "
                    f"(Transport: ${r.get('transport_charge', 0):.2f}, "
                    f"Fuel: ${r.get('fuel_surcharge', 0):.2f})"
                )
            else:
                response["answer"] = f"No shipment found matching '{inv_no}'."
            response["data"] = data
            response["type"] = "shipment_detail"

    # --- Most expensive ---
    elif any(x in q for x in ['most expensive', 'highest cost', 'costliest', 'biggest']):
        rows = c.execute('''
            SELECT tracking_id, destination, ship_date, gross_weight, total_cost, cost_per_kg
            FROM shipments WHERE total_cost > 0
            ORDER BY total_cost DESC LIMIT 5
        ''').fetchall()
        data = [dict(r) for r in rows]
        if data:
            top = data[0]
            response["answer"] = (
                f"Most expensive shipment: {top['tracking_id']} to {top['destination']} "
                f"at ${top['total_cost']:.2f} ({top['gross_weight']} kg, "
                f"${top['cost_per_kg']:.2f}/kg)."
            )
        else:
            response["answer"] = "No shipment cost data found yet."
        response["data"] = data
        response["type"] = "shipment_list"

    # --- Monthly total ---
    elif any(x in q for x in ['monthly', 'this month', 'march', 'february', 'january', 'last month']):
        rows = c.execute('''
            SELECT strftime('%Y-%m', ship_date) as month,
                   COUNT(*) as count,
                   ROUND(SUM(total_cost), 2) as total,
                   ROUND(AVG(total_cost), 2) as avg
            FROM shipments
            WHERE ship_date IS NOT NULL AND total_cost > 0
            GROUP BY month ORDER BY month DESC LIMIT 6
        ''').fetchall()
        data = [dict(r) for r in rows]
        if data:
            lines = [f"{r['month']}: ${r['total']:.2f} ({r['count']} shipments)" for r in data]
            response["answer"] = "Monthly shipping costs:\n" + "\n".join(lines)
        else:
            response["answer"] = "No monthly data available yet."
        response["data"] = data
        response["type"] = "monthly_table"

    # --- Summary / overview ---
    elif any(x in q for x in ['summary', 'overview', 'total', 'how many', 'all shipments']):
        stats = c.execute('''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status='matched' THEN 1 ELSE 0 END) as matched,
                   SUM(CASE WHEN status='unmatched' THEN 1 ELSE 0 END) as unmatched,
                   ROUND(SUM(total_cost), 2) as total_spend,
                   ROUND(AVG(cost_per_kg), 2) as avg_per_kg
            FROM shipments
        ''').fetchone()
        s = dict(stats)
        response["answer"] = (
            f"Database summary: {s['total']} total shipments — "
            f"{s['matched']} matched, {s['unmatched']} unmatched. "
            f"Total UPS spend: ${s['total_spend'] or 0:.2f}. "
            f"Average cost per KG: ${s['avg_per_kg'] or 0:.2f}."
        )
        response["data"] = [s]
        response["type"] = "summary"

    # --- Default fallback ---
    else:
        rows = c.execute('''
            SELECT tracking_id, destination, ship_date, total_cost, status
            FROM shipments ORDER BY created_at DESC LIMIT 10
        ''').fetchall()
        data = [dict(r) for r in rows]
        response["answer"] = (
            f"I found {len(data)} recent shipments in your database. "
            "Try asking: 'missing POD', 'highest fuel surcharge', "
            "'average cost per KG', 'most expensive shipment', or a specific invoice number."
        )
        response["data"] = data
        response["type"] = "shipment_list"

    conn.close()
    return response
