import os
import shutil
import json
from datetime import datetime
from backend.database import get_conn

BASE_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'shipments')


def organize_shipment(shipment_id: int) -> dict:
    """Create folder structure for a shipment and copy documents."""
    conn = get_conn()
    c = conn.cursor()

    row = c.execute('''
        SELECT s.*, ei.invoice_number as exp_inv_no,
               d1.filepath as exp_doc_path,
               ui.ups_invoice_number,
               d2.filepath as ups_doc_path
        FROM shipments s
        LEFT JOIN export_invoices ei ON s.export_invoice_id = ei.id
        LEFT JOIN documents d1 ON ei.document_id = d1.id
        LEFT JOIN ups_invoices ui ON s.ups_invoice_id = ui.id
        LEFT JOIN documents d2 ON ui.document_id = d2.id
        WHERE s.id = ?
    ''', (shipment_id,)).fetchone()

    if not row:
        conn.close()
        return {"error": "Shipment not found"}

    s = dict(row)
    tracking_id = s['tracking_id'] or f"SHIPMENT_{shipment_id}"

    # Determine year/month from ship date
    try:
        if s['ship_date']:
            dt = datetime.strptime(s['ship_date'][:10], '%Y-%m-%d')
        else:
            dt = datetime.now()
    except Exception:
        dt = datetime.now()

    year = dt.strftime('%Y')
    month = dt.strftime('%B')

    folder = os.path.join(BASE_DIR, year, month, tracking_id)
    os.makedirs(folder, exist_ok=True)
    os.makedirs(os.path.join(folder, 'screenshots'), exist_ok=True)

    # Copy export invoice
    if s.get('exp_doc_path') and os.path.exists(s['exp_doc_path']):
        shutil.copy2(s['exp_doc_path'], os.path.join(folder, 'export_invoice.pdf'))

    # Copy UPS invoice
    if s.get('ups_doc_path') and os.path.exists(s['ups_doc_path']):
        shutil.copy2(s['ups_doc_path'], os.path.join(folder, 'ups_invoice.pdf'))

    # Create notes file
    notes_path = os.path.join(folder, 'notes.txt')
    if not os.path.exists(notes_path):
        with open(notes_path, 'w') as f:
            f.write(f"Shipment Notes — {tracking_id}\n")
            f.write(f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
            f.write("Add your notes here.\n")

    # Write master.json
    master = {
        "tracking_id": tracking_id,
        "export_invoice": s.get('exp_inv_no'),
        "ups_invoice": s.get('ups_invoice_number'),
        "ship_date": s.get('ship_date'),
        "destination": s.get('destination'),
        "consignee": s.get('consignee'),
        "gross_weight_kg": s.get('gross_weight'),
        "total_cost_usd": s.get('total_cost'),
        "cost_per_kg": s.get('cost_per_kg'),
        "status": s.get('status'),
        "folder": folder,
        "last_updated": datetime.now().isoformat(),
        "documents": {
            "export_invoice": os.path.exists(os.path.join(folder, 'export_invoice.pdf')),
            "ups_invoice": os.path.exists(os.path.join(folder, 'ups_invoice.pdf')),
            "pod": os.path.exists(os.path.join(folder, 'pod.pdf')),
            "customs": os.path.exists(os.path.join(folder, 'customs_clearance.pdf')),
        }
    }

    with open(os.path.join(folder, 'master.json'), 'w') as f:
        json.dump(master, f, indent=2)

    # Update DB with folder path
    c.execute('UPDATE shipments SET folder_path = ? WHERE id = ?', (folder, shipment_id))
    conn.commit()
    conn.close()

    return {"folder": folder, "tracking_id": tracking_id, "master": master}


def organize_all_shipments():
    """Organize all matched shipments into folders."""
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT id FROM shipments WHERE status = 'matched'").fetchall()
    conn.close()

    results = []
    for row in rows:
        result = organize_shipment(row['id'])
        results.append(result)
    return results


def get_folder_tree():
    """Return the folder tree structure as a dict."""
    tree = {}
    if not os.path.exists(BASE_DIR):
        return tree

    for year in sorted(os.listdir(BASE_DIR), reverse=True):
        year_path = os.path.join(BASE_DIR, year)
        if not os.path.isdir(year_path):
            continue
        tree[year] = {}
        for month in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue
            tree[year][month] = []
            for tracking in sorted(os.listdir(month_path)):
                tracking_path = os.path.join(month_path, tracking)
                if not os.path.isdir(tracking_path):
                    continue
                docs = os.listdir(tracking_path)
                tree[year][month].append({
                    "tracking_id": tracking,
                    "path": tracking_path,
                    "documents": docs
                })
    return tree
