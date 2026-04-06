from backend.database import get_conn


def get_dashboard_stats():
    conn = get_conn()
    c = conn.cursor()
    stats = {}
    row = c.execute('''
        SELECT COUNT(*) as total,
               SUM(CASE WHEN status='matched' THEN 1 ELSE 0 END) as matched,
               SUM(CASE WHEN status='unmatched' OR ups_invoice_id IS NULL THEN 1 ELSE 0 END) as missing_ups,
               ROUND(SUM(total_cost), 2) as total_spend,
               ROUND(AVG(cost_per_kg), 2) as avg_per_kg,
               ROUND(SUM(gross_weight), 2) as total_weight
        FROM shipments
    ''').fetchone()
    stats.update(dict(row))

    reminders = c.execute(
        "SELECT COUNT(*) as due FROM reminders WHERE is_sent=0 AND is_enabled=1 AND due_date <= date('now')"
    ).fetchone()
    stats['due_reminders'] = reminders['due']

    # Top destination by cost
    top_dest = c.execute('''
        SELECT destination, ROUND(SUM(total_cost),2) as total
        FROM shipments WHERE destination IS NOT NULL AND total_cost > 0
        GROUP BY destination ORDER BY total DESC LIMIT 1
    ''').fetchone()
    stats['top_destination'] = dict(top_dest) if top_dest else None

    # This month vs last month
    this_month = c.execute('''
        SELECT ROUND(SUM(total_cost),2) as total, COUNT(*) as count
        FROM shipments
        WHERE strftime('%Y-%m', ship_date) = strftime('%Y-%m', 'now')
    ''').fetchone()
    stats['this_month'] = dict(this_month) if this_month else {'total': 0, 'count': 0}

    last_month = c.execute('''
        SELECT ROUND(SUM(total_cost),2) as total, COUNT(*) as count
        FROM shipments
        WHERE strftime('%Y-%m', ship_date) = strftime('%Y-%m', date('now', '-1 month'))
    ''').fetchone()
    stats['last_month'] = dict(last_month) if last_month else {'total': 0, 'count': 0}

    conn.close()
    return stats


def get_monthly_costs(months: int = 12):
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute('''
        SELECT strftime('%Y-%m', ship_date) as month,
               ROUND(SUM(total_cost), 2) as total,
               COUNT(*) as count,
               ROUND(AVG(cost_per_kg), 2) as avg_per_kg
        FROM shipments
        WHERE ship_date IS NOT NULL AND total_cost > 0
        GROUP BY month
        ORDER BY month DESC LIMIT ?
    ''', (months,)).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


def get_cost_by_country():
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute('''
        SELECT destination,
               COUNT(*) as count,
               ROUND(AVG(total_cost), 2) as avg_cost,
               ROUND(AVG(cost_per_kg), 2) as avg_per_kg,
               ROUND(SUM(total_cost), 2) as total_cost,
               ROUND(AVG(gross_weight), 2) as avg_weight
        FROM shipments
        WHERE total_cost > 0 AND destination IS NOT NULL
        GROUP BY destination
        ORDER BY total_cost DESC
    ''').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_charge_composition():
    conn = get_conn()
    c = conn.cursor()
    row = c.execute('''
        SELECT ROUND(SUM(transport_charge), 2) as transport,
               ROUND(SUM(fuel_surcharge), 2) as fuel,
               ROUND(SUM(remote_area_surcharge), 2) as remote,
               ROUND(SUM(duty_tax), 2) as duty,
               ROUND(SUM(other_charges), 2) as other
        FROM ups_invoices
    ''').fetchone()
    conn.close()
    if row:
        return dict(row)
    return {"transport": 0, "fuel": 0, "remote": 0, "duty": 0, "other": 0}


def get_fuel_trend(months: int = 12):
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute('''
        SELECT strftime('%Y-%m', s.ship_date) as month,
               ROUND(AVG(ui.fuel_surcharge * 100.0 / NULLIF(ui.total_charge, 0)), 1) as fuel_pct,
               ROUND(AVG(ui.fuel_surcharge), 2) as avg_fuel_amt
        FROM shipments s
        JOIN ups_invoices ui ON s.ups_invoice_id = ui.id
        WHERE s.ship_date IS NOT NULL
        GROUP BY month
        ORDER BY month DESC LIMIT ?
    ''', (months,)).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


def get_recent_shipments(limit: int = 20):
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute('''
        SELECT s.id, s.tracking_id, s.ship_date, s.destination,
               s.consignee, s.gross_weight, s.total_cost, s.cost_per_kg,
               s.status, s.tags, s.priority,
               ei.invoice_number as export_invoice,
               ui.ups_invoice_number,
               ui.transport_charge, ui.fuel_surcharge,
               ui.remote_area_surcharge, ui.duty_tax, ui.service_type
        FROM shipments s
        LEFT JOIN export_invoices ei ON s.export_invoice_id = ei.id
        LEFT JOIN ups_invoices ui ON s.ups_invoice_id = ui.id
        ORDER BY s.created_at DESC LIMIT ?
    ''', (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_shipment_detail(shipment_id: int):
    conn = get_conn()
    c = conn.cursor()
    row = c.execute('''
        SELECT s.*, ei.invoice_number as export_invoice, ei.product_desc,
               ei.declared_value, ei.destination_country,
               ui.ups_invoice_number, ui.transport_charge, ui.fuel_surcharge,
               ui.remote_area_surcharge, ui.duty_tax, ui.other_charges,
               ui.service_type, ui.billed_weight
        FROM shipments s
        LEFT JOIN export_invoices ei ON s.export_invoice_id = ei.id
        LEFT JOIN ups_invoices ui ON s.ups_invoice_id = ui.id
        WHERE s.id = ?
    ''', (shipment_id,)).fetchone()

    # Also get reminders
    reminders = c.execute(
        'SELECT * FROM reminders WHERE shipment_id = ? ORDER BY due_date ASC',
        (shipment_id,)
    ).fetchall()
    conn.close()

    if row:
        result = dict(row)
        result['reminders'] = [dict(r) for r in reminders]
        return result
    return None


def get_alerts():
    conn = get_conn()
    c = conn.cursor()
    alerts = []

    unmatched = c.execute(
        "SELECT tracking_id FROM shipments WHERE ups_invoice_id IS NULL LIMIT 5"
    ).fetchall()
    for r in unmatched:
        alerts.append({"type": "error", "message": f"No UPS match for shipment {r['tracking_id']}"})

    due = c.execute('''
        SELECT r.reminder_type, s.tracking_id
        FROM reminders r JOIN shipments s ON r.shipment_id = s.id
        WHERE r.is_sent = 0 AND r.due_date <= date('now') LIMIT 5
    ''').fetchall()
    for r in due:
        alerts.append({"type": "warning", "message": f"{r['reminder_type'].upper()} overdue for {r['tracking_id']}"})

    # High cost alerts (above avg by 2x)
    avg_row = c.execute("SELECT AVG(total_cost) as avg FROM shipments WHERE total_cost > 0").fetchone()
    if avg_row and avg_row['avg']:
        high_cost = c.execute(
            "SELECT tracking_id, total_cost FROM shipments WHERE total_cost > ? ORDER BY total_cost DESC LIMIT 3",
            (avg_row['avg'] * 2,)
        ).fetchall()
        for r in high_cost:
            alerts.append({"type": "info", "message": f"High cost shipment: {r['tracking_id']} (${r['total_cost']:.2f})"})

    conn.close()
    return alerts


def search_shipments(query: str, status: str = None, destination: str = None,
                     date_from: str = None, date_to: str = None,
                     min_cost: float = None, max_cost: float = None,
                     limit: int = 50):
    conn = get_conn()
    c = conn.cursor()

    sql = '''
        SELECT s.id, s.tracking_id, s.ship_date, s.destination,
               s.consignee, s.gross_weight, s.total_cost, s.cost_per_kg,
               s.status, s.tags, s.priority,
               ei.invoice_number as export_invoice,
               ui.ups_invoice_number, ui.service_type
        FROM shipments s
        LEFT JOIN export_invoices ei ON s.export_invoice_id = ei.id
        LEFT JOIN ups_invoices ui ON s.ups_invoice_id = ui.id
        WHERE 1=1
    '''
    params = []

    if query:
        sql += ''' AND (
            s.tracking_id LIKE ? OR s.consignee LIKE ? OR s.destination LIKE ?
            OR ei.invoice_number LIKE ? OR ui.ups_invoice_number LIKE ?
        )'''
        q = f'%{query}%'
        params.extend([q, q, q, q, q])

    if status:
        sql += ' AND s.status = ?'
        params.append(status)
    if destination:
        sql += ' AND s.destination = ?'
        params.append(destination)
    if date_from:
        sql += ' AND s.ship_date >= ?'
        params.append(date_from)
    if date_to:
        sql += ' AND s.ship_date <= ?'
        params.append(date_to)
    if min_cost is not None:
        sql += ' AND s.total_cost >= ?'
        params.append(min_cost)
    if max_cost is not None:
        sql += ' AND s.total_cost <= ?'
        params.append(max_cost)

    sql += ' ORDER BY s.created_at DESC LIMIT ?'
    params.append(limit)

    rows = c.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_destinations():
    conn = get_conn()
    rows = conn.execute(
        "SELECT DISTINCT destination FROM shipments WHERE destination IS NOT NULL ORDER BY destination"
    ).fetchall()
    conn.close()
    return [r['destination'] for r in rows]


def get_cost_efficiency_report():
    """Top/bottom performers by cost per kg."""
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute('''
        SELECT tracking_id, destination, ship_date, gross_weight, total_cost, cost_per_kg, service_type
        FROM shipments s
        LEFT JOIN ups_invoices ui ON s.ups_invoice_id = ui.id
        WHERE cost_per_kg > 0
        ORDER BY cost_per_kg DESC
    ''').fetchall()
    conn.close()
    data = [dict(r) for r in rows]
    if not data:
        return {"best": [], "worst": []}
    return {"worst": data[:5], "best": data[-5:]}
