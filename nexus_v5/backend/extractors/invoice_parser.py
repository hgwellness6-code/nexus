import re
from datetime import datetime


def parse_export_invoice(text: str) -> dict:
    """
    Extract structured fields from export invoice text.
    Returns dict of fields with confidence score.
    """
    fields = {
        "invoice_number": None,
        "invoice_date": None,
        "consignee": None,
        "destination_country": None,
        "tracking_id": None,
        "gross_weight": None,
        "chargeable_weight": None,
        "declared_value": None,
        "currency": "USD",
        "product_desc": None,
        "confidence": 0.0,
    }

    found = 0
    total = 8

    # ── Invoice number
    inv_patterns = [
        r'invoice\s*(?:no|number|#)[.:\s]*([A-Z0-9][A-Z0-9\-\/]+)',
        r'inv[.\s]*no[:\s]*([A-Z0-9\-\/]+)',
        r'(?:export|commercial)\s+invoice[:\s#]*([A-Z0-9\-\/]+)',
    ]
    for p in inv_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if len(val) > 3:
                fields["invoice_number"] = val
                found += 1
                break

    # ── Invoice date — supports numeric AND text month (12 April 2024)
    date_patterns = [
        r'(?:invoice\s+date|date)[:\s]+(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})',
        r'(?:invoice\s+date|date)[:\s]+(\d{1,2}\s+\w+\s+\d{4})',
        r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
    ]
    for p in date_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            fields["invoice_date"] = m.group(1).strip()
            found += 1
            break

    # ── Tracking ID
    tracking_patterns = [
        r'(1Z[A-Z0-9]{16})',
        r'tracking\s*(?:no|number|id|#)[:\s]*([A-Z0-9]{10,25})',
        r'awb[:\s#]*([A-Z0-9\-]{8,20})',
        r'airway\s*bill[:\s#]*([A-Z0-9\-]{8,20})',
    ]
    for p in tracking_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            fields["tracking_id"] = m.group(1).strip().upper()
            found += 1
            break

    # ── Consignee — try explicit CONSIGNEE section heading first
    cons_m = re.search(r'CONSIGNEE\s*\n([^\n]+)', text, re.IGNORECASE)
    if cons_m:
        val = cons_m.group(1).strip()[:80]
        if len(val) > 3:
            fields["consignee"] = val
            found += 1
    else:
        # Fallback: inline patterns
        for p in [
            r'consignee[:\s]+([A-Za-z0-9\s,\.]+?)(?:\n|ship|address)',
            r'(?:bill\s+to|buyer)[:\s]+([A-Za-z0-9\s,\.]+?)(?:\n)',
        ]:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                val = m.group(1).strip()[:80]
                if len(val) > 3:
                    fields["consignee"] = val
                    found += 1
                    break

    # ── Destination country
    # Priority 1: explicit "DESTINATION COUNTRY" section
    dest_section = re.search(
        r'destination\s*country[:\s\n]+([A-Za-z ]+?)(?:\n|$)',
        text, re.IGNORECASE
    )
    if dest_section:
        raw = dest_section.group(1).strip()
        country_map = {
            'united states': 'USA', 'usa': 'USA', 'us': 'USA',
            'uk': 'GBR', 'united kingdom': 'GBR',
            'australia': 'AUS', 'germany': 'DEU', 'canada': 'CAN',
            'france': 'FRA', 'japan': 'JPN', 'china': 'CHN',
            'india': 'IND', 'singapore': 'SGP', 'netherlands': 'NLD',
            'italy': 'ITA', 'spain': 'ESP', 'brazil': 'BRA',
            'mexico': 'MEX', 'south korea': 'KOR',
        }
        fields["destination_country"] = country_map.get(raw.lower(), raw.upper()[:3])
        found += 1
    else:
        # Priority 2: look for country in CONSIGNEE address block (after consignee heading)
        # Extract only the text after CONSIGNEE section to avoid matching shipper's country
        consignee_block = ''
        cons_start = re.search(r'CONSIGNEE', text, re.IGNORECASE)
        dest_start = re.search(r'DESTINATION', text, re.IGNORECASE)
        if cons_start and dest_start:
            consignee_block = text[cons_start.start():dest_start.start()]
        elif cons_start:
            consignee_block = text[cons_start.start():cons_start.start() + 300]

        search_text = consignee_block if consignee_block else text
        countries = ['USA', 'United States', 'UK', 'United Kingdom', 'Australia',
                     'Germany', 'Canada', 'France', 'Japan', 'China', 'Singapore',
                     'UAE', 'Netherlands', 'Italy', 'Spain', 'Brazil', 'Mexico',
                     'South Korea']
        country_map = {
            'United States': 'USA', 'UK': 'GBR', 'United Kingdom': 'GBR',
            'Australia': 'AUS', 'Germany': 'DEU', 'Canada': 'CAN', 'France': 'FRA',
            'Japan': 'JPN', 'China': 'CHN', 'India': 'IND', 'Singapore': 'SGP',
            'UAE': 'ARE', 'Netherlands': 'NLD', 'Italy': 'ITA', 'Spain': 'ESP',
            'Brazil': 'BRA', 'Mexico': 'MEX', 'South Korea': 'KOR',
        }
        for country in countries:
            if re.search(r'\b' + country + r'\b', search_text, re.IGNORECASE):
                fields["destination_country"] = country_map.get(country, country.upper()[:3])
                found += 1
                break

    # ── Gross weight
    weight_patterns = [
        r'gross\s*weight[:\s]*([\d,\.]+)\s*(?:kg|kgs|kilogram)',
        r'net\s*weight[:\s]*([\d,\.]+)\s*(?:kg|kgs)',
        r'weight[:\s]*([\d,\.]+)\s*(?:kg|kgs)',
        r'([\d,\.]+)\s*kgs?\b',
    ]
    for p in weight_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            try:
                fields["gross_weight"] = float(m.group(1).replace(',', ''))
                found += 1
                break
            except ValueError:
                pass

    # ── Chargeable weight
    chg_patterns = [
        r'chargeable?\s*weight[:\s]*([\d,\.]+)\s*(?:kg|kgs)?',
        r'volumetric\s*weight[:\s]*([\d,\.]+)',
    ]
    for p in chg_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            try:
                fields["chargeable_weight"] = float(m.group(1).replace(',', ''))
                break
            except ValueError:
                pass

    # ── Declared / invoice value
    # Handles Indian number formatting (4,78,930.00) by stripping all commas
    value_patterns = [
        r'(?:total|invoice|declared|fob)\s*(?:value|amount)[:\s]*(?:USD|INR|EUR|GBP)?\s*([\d,\.]+)',
        r'(?:USD|INR|EUR|GBP)\s*([\d,\.]+)',
        r'(?:value)[:\s]*([\d,\.]+)',
    ]
    for p in value_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1).replace(',', ''))
                if val > 0:
                    fields["declared_value"] = val
                    found += 1
                    break
            except ValueError:
                pass

    # ── Currency
    curr_m = re.search(r'\b(USD|INR|EUR|GBP|AUD|CAD|SGD|JPY)\b', text)
    if curr_m:
        fields["currency"] = curr_m.group(1)

    # ── Product description
    desc_m = re.search(
        r'(?:description|goods|product)[:\s]+([A-Za-z0-9\s\-,\.]{5,100})',
        text, re.IGNORECASE
    )
    if desc_m:
        fields["product_desc"] = desc_m.group(1).strip()[:100]

    fields["confidence"] = round(found / total, 2)
    return fields
