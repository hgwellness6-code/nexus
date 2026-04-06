import re


def parse_ups_invoice(text: str) -> dict:
    """
    Extract structured fields from UPS invoice text.
    Handles both single-currency (USD) and dual-column (USD + INR) invoices.
    Returns dict of fields with confidence score.
    """
    fields = {
        "ups_invoice_number": None,
        "invoice_date": None,
        "tracking_number": None,
        "service_type": None,
        "billed_weight": None,
        "transport_charge": 0.0,
        "fuel_surcharge": 0.0,
        "remote_area_surcharge": 0.0,
        "duty_tax": 0.0,
        "other_charges": 0.0,
        "total_charge": 0.0,
        "currency": "USD",
        "confidence": 0.0,
    }

    found = 0
    total = 7

    # ── Currency (detect early, needed for dual-column logic)
    curr_m = re.search(r'\b(USD|INR|EUR|GBP|AUD|CAD|SGD)\b', text)
    if curr_m:
        fields["currency"] = curr_m.group(1)

    # ── Detect dual-column invoice (has exchange rate line = USD + local currency columns)
    is_dual_column = bool(re.search(r'exchange\s*rate', text, re.IGNORECASE))

    # ── UPS Invoice number
    # Handles: "UPS Invoice No.: UPS-INV-2024-0412" and plain "Invoice No: ABC-123"
    inv_patterns = [
        r'ups\s*invoice\s*(?:no|number|#)[.:\s]*([A-Z0-9][A-Z0-9\-]+)',
        r'invoice\s*(?:no|number|#)[.:\s]*([A-Z0-9][A-Z0-9\-]+)',
        r'account\s*invoice[:\s]*([A-Z0-9][A-Z0-9\-]+)',
    ]
    for p in inv_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if len(val) > 3:  # skip short noise like 'No' itself
                fields["ups_invoice_number"] = val
                found += 1
                break

    # ── Invoice date — supports numeric (12/04/2024) AND text month (14 April 2024)
    date_patterns = [
        r'(?:invoice|bill)\s*date[:\s]+(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})',
        r'(?:invoice|bill)\s*date[:\s]+(\d{1,2}\s+\w+\s+\d{4})',
        r'date[:\s]+(\d{1,2}\s+\w+\s+\d{4})',
        r'date[:\s]+(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})',
    ]
    for p in date_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            fields["invoice_date"] = m.group(1).strip()
            found += 1
            break

    # ── Tracking number
    track_m = re.search(r'(1Z[A-Z0-9]{16})', text, re.IGNORECASE)
    if track_m:
        fields["tracking_number"] = track_m.group(1).strip().upper()
        found += 1
    else:
        track_m2 = re.search(
            r'tracking\s*(?:no|number|#)[:\s]*([A-Z0-9]{10,25})',
            text, re.IGNORECASE
        )
        if track_m2:
            fields["tracking_number"] = track_m2.group(1).strip().upper()
            found += 1

    # ── Service type
    services = ['Worldwide Express', 'Worldwide Saver', 'Worldwide Expedited',
                'Standard', 'Express Plus', 'Ground', 'Air', 'Freight']
    for svc in services:
        if re.search(svc, text, re.IGNORECASE):
            fields["service_type"] = 'UPS ' + svc
            found += 1
            break

    # ── Billed weight
    bw_m = re.search(
        r'(?:billed|chargeable|billable)\s*weight[:\s]*([\d,\.]+)\s*(?:kg|lbs?)?',
        text, re.IGNORECASE
    )
    if bw_m:
        try:
            fields["billed_weight"] = float(bw_m.group(1).replace(',', ''))
            found += 1
        except ValueError:
            pass

    # ── Charge helpers ──────────────────────────────────────────────────────────

    def get_charge_single(patterns, text):
        """Extract first number after the pattern label (single-currency invoice)."""
        for p in patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                try:
                    return float(m.group(1).replace(',', ''))
                except ValueError:
                    pass
        return 0.0

    def get_charge_dual(label_patterns, text):
        """
        For dual-column invoices (e.g. USD | INR), match the label then grab
        the LAST number on that line — that is the local-currency amount.
        """
        for p in label_patterns:
            m = re.search(p + r'[^\n]*', text, re.IGNORECASE)
            if m:
                line = m.group(0)
                nums = re.findall(r'[\d,]+\.?\d*', line)
                if nums:
                    try:
                        return float(nums[-1].replace(',', ''))
                    except ValueError:
                        pass
        return 0.0

    # ── Transportation charge
    if is_dual_column:
        fields["transport_charge"] = get_charge_dual(
            [r'transportation\s*charge', r'freight\s*charge',
             r'shipping\s*charge', r'base\s*charge'], text)
    else:
        fields["transport_charge"] = get_charge_single([
            r'transportation\s*charge[:\s]+([\d,\.]+)',
            r'freight\s*charge[:\s]+([\d,\.]+)',
            r'shipping\s*charge[:\s]+([\d,\.]+)',
            r'base\s*charge[:\s]+([\d,\.]+)',
        ], text)

    # ── Fuel surcharge  (skip optional "(17.5%)" between label and number)
    if is_dual_column:
        fields["fuel_surcharge"] = get_charge_dual([r'fuel\s*surcharge'], text)
    else:
        m = re.search(
            r'fuel\s*surcharge\s*(?:\([^)]*\))?\s*([\d,\.]+)',
            text, re.IGNORECASE
        )
        fields["fuel_surcharge"] = float(m.group(1).replace(',', '')) if m else 0.0

    # ── Remote area surcharge
    if is_dual_column:
        fields["remote_area_surcharge"] = get_charge_dual(
            [r'remote\s*area', r'extended\s*area',
             r'delivery\s*area\s*surcharge'], text)
    else:
        fields["remote_area_surcharge"] = get_charge_single([
            r'remote\s*area[:\s]+([\d,\.]+)',
            r'extended\s*area[:\s]+([\d,\.]+)',
            r'delivery\s*area\s*surcharge[:\s]+([\d,\.]+)',
        ], text)

    # ── Duties and taxes
    if is_dual_column:
        fields["duty_tax"] = get_charge_dual(
            [r'duty\s*(?:&|and)?\s*tax', r'import\s*tax',
             r'customs\s*charge', r'brokerage'], text)
    else:
        fields["duty_tax"] = get_charge_single([
            r'(?:duty|duties)[:\s]+([\d,\.]+)',
            r'import\s*tax[:\s]+([\d,\.]+)',
            r'customs\s*charge[:\s]+([\d,\.]+)',
            r'brokerage[:\s]+([\d,\.]+)',
        ], text)

    # ── Other charges (explicit line, dual-col grabs INR value)
    if is_dual_column:
        fields["other_charges"] = get_charge_dual([r'other\s*charges'], text)
    else:
        fields["other_charges"] = get_charge_single(
            [r'other\s*charges[:\s]+([\d,\.]+)'], text)

    # ── Total charge
    # Dual-column: "TOTAL DUE 461.50 INR 45,416.94" → grab last number on line
    total_charge = 0.0
    if is_dual_column:
        m_tot = re.search(
            r'total\s*(?:charge|amount|due)[^\n]*',
            text, re.IGNORECASE
        )
        if m_tot:
            nums = re.findall(r'[\d,]+\.?\d*', m_tot.group(0))
            if nums:
                try:
                    total_charge = float(nums[-1].replace(',', ''))
                except ValueError:
                    pass
    else:
        total_charge = get_charge_single([
            r'total\s*(?:charge|amount|due)[:\s]+([\d,\.]+)',
            r'amount\s*due[:\s]+([\d,\.]+)',
            r'invoice\s*total[:\s]+([\d,\.]+)',
        ], text)

    if total_charge > 0:
        fields["total_charge"] = total_charge
        found += 1
    else:
        # Calculate from parts
        fields["total_charge"] = round(
            fields["transport_charge"] + fields["fuel_surcharge"] +
            fields["remote_area_surcharge"] + fields["duty_tax"] +
            fields["other_charges"], 2
        )
        if fields["total_charge"] > 0:
            found += 1

    # ── Other charges fallback: total minus known line-items
    if fields["other_charges"] == 0:
        known = (fields["transport_charge"] + fields["fuel_surcharge"] +
                 fields["remote_area_surcharge"] + fields["duty_tax"])
        diff = round(fields["total_charge"] - known, 2)
        if diff > 0:
            fields["other_charges"] = diff

    fields["confidence"] = round(found / total, 2)
    return fields
