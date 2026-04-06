"""
Nexus Shipping Intelligence — PDF Report Generator
Produces a clean, modern, client-ready PDF report.
"""

import io
import os
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    BaseDocTemplate, Frame, HRFlowable, Image, NextPageTemplate,
    PageBreak, PageTemplate, Paragraph, Spacer, Table, TableStyle,
)
from reportlab.platypus.flowables import Flowable

# ── Brand colours ────────────────────────────────────────────────────────────
BLUE       = colors.HexColor("#2563EB")
BLUE_LIGHT = colors.HexColor("#DBEAFE")
BLUE_DARK  = colors.HexColor("#1D4ED8")
GREEN      = colors.HexColor("#16A34A")
GREEN_LIGHT= colors.HexColor("#DCFCE7")
AMBER      = colors.HexColor("#D97706")
AMBER_LIGHT= colors.HexColor("#FEF3C7")
RED        = colors.HexColor("#DC2626")
RED_LIGHT  = colors.HexColor("#FEE2E2")
PURPLE     = colors.HexColor("#7C3AED")
CYAN       = colors.HexColor("#0891B2")
CYAN_LIGHT = colors.HexColor("#CFFAFE")

INK        = colors.HexColor("#0D0D12")
INK2       = colors.HexColor("#3A3A4A")
INK3       = colors.HexColor("#8888A0")
LINE       = colors.HexColor("#E8E6E0")
SURFACE    = colors.HexColor("#F9F8F6")
WHITE      = colors.white

PAGE_W, PAGE_H = A4
MARGIN = 20 * mm


# ── Custom Flowables ─────────────────────────────────────────────────────────

class ColorBar(Flowable):
    """A full-width horizontal colour bar (used for section dividers)."""
    def __init__(self, height=2, color=BLUE, width=None):
        super().__init__()
        self._color = color
        self._h = height
        self._w = width

    def draw(self):
        self.canv.setFillColor(self._color)
        w = self._w or self.canv._pagesize[0]
        self.canv.rect(0, 0, w, self._h, stroke=0, fill=1)

    def wrap(self, avail_w, avail_h):
        self._w = avail_w
        return avail_w, self._h


class KPIRow(Flowable):
    """4-column KPI card row drawn directly on canvas."""
    def __init__(self, kpis, width):
        super().__init__()
        self._kpis = kpis   # list of (label, value, sub, accent_color)
        self._w = width
        self._h = 28 * mm

    def wrap(self, avail_w, avail_h):
        return self._w, self._h

    def draw(self):
        n = len(self._kpis)
        gap = 4 * mm
        card_w = (self._w - gap * (n - 1)) / n
        card_h = self._h

        for i, (label, value, sub, color) in enumerate(self._kpis):
            x = i * (card_w + gap)
            c = self.canv

            # Card background
            c.setFillColor(SURFACE)
            c.roundRect(x, 0, card_w, card_h, 4, stroke=0, fill=1)

            # Accent left border
            c.setFillColor(color)
            c.rect(x, 0, 3, card_h, stroke=0, fill=1)

            # Value
            c.setFont("Helvetica-Bold", 16)
            c.setFillColor(color)
            c.drawString(x + 8 * mm, card_h - 11 * mm, str(value))

            # Label
            c.setFont("Helvetica", 8)
            c.setFillColor(INK3)
            c.drawString(x + 8 * mm, card_h - 16 * mm, label.upper())

            # Sub text
            c.setFont("Helvetica", 7.5)
            c.setFillColor(INK3)
            c.drawString(x + 8 * mm, 4 * mm, str(sub))


class ProgressBar(Flowable):
    """Horizontal progress bar for destination breakdown."""
    def __init__(self, label, value_str, pct, color=BLUE, width=None):
        super().__init__()
        self._label = label
        self._val = value_str
        self._pct = min(max(pct, 0), 1)
        self._color = color
        self._w = width or 100 * mm
        self._h = 9 * mm

    def wrap(self, avail_w, avail_h):
        self._w = avail_w
        return avail_w, self._h

    def draw(self):
        c = self.canv
        bar_y = 3 * mm
        bar_h = 2.5 * mm
        label_w = 42 * mm
        val_w = 22 * mm
        bar_w = self._w - label_w - val_w - 4 * mm

        # Label
        c.setFont("Helvetica", 8.5)
        c.setFillColor(INK2)
        c.drawString(0, bar_y + 0.5 * mm, self._label)

        # Track
        c.setFillColor(LINE)
        c.roundRect(label_w, bar_y, bar_w, bar_h, 1.5, stroke=0, fill=1)

        # Fill
        if self._pct > 0:
            c.setFillColor(self._color)
            c.roundRect(label_w, bar_y, bar_w * self._pct, bar_h, 1.5, stroke=0, fill=1)

        # Value
        c.setFont("Helvetica-Bold", 8.5)
        c.setFillColor(INK)
        c.drawRightString(self._w, bar_y + 0.5 * mm, self._val)


# ── Style helpers ─────────────────────────────────────────────────────────────

def _styles():
    base = getSampleStyleSheet()
    return {
        "h1": ParagraphStyle("h1", fontName="Helvetica-Bold", fontSize=22,
                             textColor=INK, leading=28, spaceAfter=2),
        "h2": ParagraphStyle("h2", fontName="Helvetica-Bold", fontSize=13,
                             textColor=INK, leading=18, spaceBefore=6, spaceAfter=4),
        "h3": ParagraphStyle("h3", fontName="Helvetica-Bold", fontSize=10,
                             textColor=INK2, leading=14, spaceBefore=4, spaceAfter=2),
        "body": ParagraphStyle("body", fontName="Helvetica", fontSize=9,
                               textColor=INK2, leading=14),
        "small": ParagraphStyle("small", fontName="Helvetica", fontSize=7.5,
                                textColor=INK3, leading=11),
        "label": ParagraphStyle("label", fontName="Helvetica-Bold", fontSize=7,
                                textColor=INK3, leading=10,
                                wordWrap="CJK"),
        "mono": ParagraphStyle("mono", fontName="Courier", fontSize=8,
                               textColor=INK2, leading=12),
        "pill_blue": ParagraphStyle("pill_blue", fontName="Helvetica-Bold",
                                    fontSize=7.5, textColor=BLUE, leading=10),
        "pill_green": ParagraphStyle("pill_green", fontName="Helvetica-Bold",
                                     fontSize=7.5, textColor=GREEN, leading=10),
        "pill_red": ParagraphStyle("pill_red", fontName="Helvetica-Bold",
                                   fontSize=7.5, textColor=RED, leading=10),
        "center": ParagraphStyle("center", fontName="Helvetica", fontSize=9,
                                 textColor=INK3, alignment=TA_CENTER),
    }


def _fmt(val, symbol="$"):
    if val is None or val == 0:
        return "—"
    return f"{symbol}{float(val):,.2f}"


def _fmtkg(val):
    if val is None:
        return "—"
    return f"{float(val):.1f} kg"


def _fmtdate(d):
    if not d:
        return "—"
    try:
        return datetime.strptime(d[:10], "%Y-%m-%d").strftime("%d %b %Y")
    except Exception:
        return str(d)


def _status_text(s):
    return {"matched": "Matched", "unmatched": "Unmatched",
            "pending": "Pending", "closed": "Closed"}.get(s, s or "—")


def _status_color(s):
    return {"matched": GREEN, "unmatched": RED,
            "pending": AMBER, "closed": INK3}.get(s, INK3)


# ── Page templates ────────────────────────────────────────────────────────────

def _build_doc(buf, title, company="Nexus Shipping Intelligence"):
    doc = BaseDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=MARGIN,
        bottomMargin=MARGIN + 8 * mm,
        title=title,
        author=company,
    )

    def cover_page(canv, doc):
        pass  # cover has its own background drawn inline

    def _header_footer(canv, doc):
        canv.saveState()
        w, h = A4

        # Top rule
        canv.setStrokeColor(LINE)
        canv.setLineWidth(0.5)
        canv.line(MARGIN, h - MARGIN + 4 * mm, w - MARGIN, h - MARGIN + 4 * mm)

        # Header: left — company name, right — report title
        canv.setFont("Helvetica-Bold", 7.5)
        canv.setFillColor(BLUE)
        canv.drawString(MARGIN, h - MARGIN + 6 * mm, "NEXUS")
        canv.setFont("Helvetica", 7.5)
        canv.setFillColor(INK3)
        canv.drawString(MARGIN + 14 * mm, h - MARGIN + 6 * mm, "Shipping Intelligence")

        canv.setFont("Helvetica", 7.5)
        canv.setFillColor(INK3)
        canv.drawRightString(w - MARGIN, h - MARGIN + 6 * mm, title)

        # Footer rule
        canv.setStrokeColor(LINE)
        canv.line(MARGIN, MARGIN - 2 * mm, w - MARGIN, MARGIN - 2 * mm)

        # Footer: page number + generated date
        canv.setFont("Helvetica", 7)
        canv.setFillColor(INK3)
        canv.drawString(MARGIN, MARGIN - 6 * mm,
                        f"Generated {datetime.now().strftime('%d %B %Y, %H:%M')}")
        canv.drawRightString(w - MARGIN, MARGIN - 6 * mm,
                             f"Page {doc.page}")
        canv.restoreState()

    content_frame = Frame(
        MARGIN, MARGIN,
        PAGE_W - 2 * MARGIN,
        PAGE_H - 2 * MARGIN - 4 * mm,
        id="content",
    )
    cover_frame = Frame(
        0, 0, PAGE_W, PAGE_H, id="cover",
        leftPadding=0, rightPadding=0,
        topPadding=0, bottomPadding=0,
    )

    doc.addPageTemplates([
        PageTemplate(id="cover", frames=[cover_frame], onPage=cover_page),
        PageTemplate(id="content", frames=[content_frame], onPage=_header_footer),
    ])
    return doc


# ── Section builders ──────────────────────────────────────────────────────────

def _section_title(text, styles):
    return [
        Spacer(1, 5 * mm),
        ColorBar(height=1.5, color=BLUE),
        Spacer(1, 3 * mm),
        Paragraph(text, styles["h2"]),
    ]


def _cover(story, stats, period_label, styles):
    w, h = A4

    # Blue header band — use a wide table to fake full bleed
    cover_tbl = Table(
        [[Paragraph("", styles["body"])]],
        colWidths=[PAGE_W],
        rowHeights=[80 * mm],
    )
    cover_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BLUE),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    story.append(cover_tbl)

    # Overlay text on cover — embed in a new table with blue bg
    header_data = [[
        Paragraph(
            '<font color="white" size="28"><b>Nexus</b></font>'
            '<font color="#93C5FD" size="28"> Shipping</font>',
            ParagraphStyle("ch", fontName="Helvetica-Bold", fontSize=28,
                           textColor=WHITE, leading=34),
        )
    ], [
        Paragraph(
            '<font color="#93C5FD" size="11">Intelligence Report</font>',
            ParagraphStyle("cs", fontName="Helvetica", fontSize=11,
                           textColor=colors.HexColor("#93C5FD"), leading=14),
        )
    ], [
        Paragraph(
            f'<font color="#BFDBFE" size="9">{period_label}</font>',
            ParagraphStyle("cp", fontName="Helvetica", fontSize=9,
                           textColor=colors.HexColor("#BFDBFE"), leading=12),
        )
    ]]
    tbl2 = Table(header_data, colWidths=[PAGE_W - 2 * MARGIN])
    tbl2.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), MARGIN),
        ("TOPPADDING", (0, 0), (0, 0), 18 * mm),
        ("TOPPADDING", (0, 1), (0, 1), 2 * mm),
        ("TOPPADDING", (0, 2), (0, 2), 3 * mm),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    # Combine: use a neg spacer trick — just rebuild
    # We'll do this via offset on the cover_tbl (negative spacer)
    story.append(Spacer(1, -80 * mm))
    story.append(tbl2)
    story.append(Spacer(1, 18 * mm))

    # KPIs on cover
    s = stats
    kpis = [
        ("Total Shipments",  str(s.get("total") or 0),       "All time",        BLUE),
        ("Total Spend",      _fmt(s.get("total_spend")),      "UPS charges",     GREEN),
        ("Avg Cost / KG",    _fmt(s.get("avg_per_kg")),       "Across shipments",CYAN),
        ("Matched",          str(s.get("matched") or 0),      f"of {s.get('total') or 0} shipments", PURPLE),
    ]
    story.append(KPIRow(kpis, PAGE_W - 2 * MARGIN))
    story.append(Spacer(1, 10 * mm))

    # This month vs last month comparison box
    tm = s.get("this_month", {}) or {}
    lm = s.get("last_month", {}) or {}
    tm_total = tm.get("total") or 0
    lm_total = lm.get("total") or 0
    pct = ((tm_total - lm_total) / lm_total * 100) if lm_total else 0
    direction = "▲" if pct >= 0 else "▼"
    pct_color = RED if pct > 0 else GREEN

    mom_data = [
        [
            Paragraph("This Month", styles["label"]),
            Paragraph("Last Month", styles["label"]),
            Paragraph("Month-on-Month", styles["label"]),
        ],
        [
            Paragraph(f"<b>{_fmt(tm_total)}</b>  ({tm.get('count') or 0} shipments)",
                      styles["body"]),
            Paragraph(f"<b>{_fmt(lm_total)}</b>  ({lm.get('count') or 0} shipments)",
                      styles["body"]),
            Paragraph(
                f'<font color="{"#DC2626" if pct>=0 else "#16A34A"}">'
                f'<b>{direction} {abs(pct):.1f}%</b></font>',
                styles["body"],
            ),
        ],
    ]
    mom_tbl = Table(mom_data, colWidths=[(PAGE_W - 2 * MARGIN) / 3] * 3)
    mom_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), SURFACE),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EFF6FF")),
        ("TEXTCOLOR", (0, 0), (-1, 0), INK3),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 7),
        ("FONTNAME", (0, 1), (-1, 1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, 1), 9),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.HexColor("#EFF6FF"), WHITE]),
        ("BOX", (0, 0), (-1, -1), 0.5, LINE),
        ("LINEAFTER", (0, 0), (1, -1), 0.5, LINE),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("ROUNDEDCORNERS", [4, 4, 4, 4]),
    ]))
    story.append(mom_tbl)
    story.append(Spacer(1, 8 * mm))

    # Top destination
    td = s.get("top_destination")
    if td:
        story.append(Paragraph(
            f"Top Destination:  <b>{td['destination']}</b>  —  {_fmt(td['total'])} total spend",
            styles["body"],
        ))

    story.append(Spacer(1, 6 * mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=LINE))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph(
        "This report was generated automatically by Nexus Shipping Intelligence. "
        "All figures are sourced directly from your uploaded UPS invoices and export invoices.",
        styles["small"],
    ))


def _shipments_section(story, shipments, styles):
    story += _section_title("Shipment Register", styles)
    story.append(Paragraph(
        f"Complete list of {len(shipments)} shipments in this period.", styles["body"]
    ))
    story.append(Spacer(1, 3 * mm))

    hdr = ["Tracking ID", "Date", "Destination", "Weight", "Transport",
           "Fuel", "Total Cost", "Status"]
    col_w = [38 * mm, 22 * mm, 32 * mm, 18 * mm, 20 * mm, 16 * mm, 20 * mm, 18 * mm]

    rows = [hdr]
    for s in shipments:
        status_txt = _status_text(s.get("status"))
        rows.append([
            Paragraph(s.get("tracking_id") or "—",
                      ParagraphStyle("m", fontName="Courier", fontSize=7, textColor=INK)),
            _fmtdate(s.get("ship_date")),
            s.get("destination") or "—",
            _fmtkg(s.get("gross_weight")),
            _fmt(s.get("transport_charge")),
            _fmt(s.get("fuel_surcharge")),
            _fmt(s.get("total_cost")),
            Paragraph(status_txt,
                      ParagraphStyle("st", fontName="Helvetica-Bold", fontSize=7,
                                     textColor=_status_color(s.get("status")))),
        ])

    tbl = Table(rows, colWidths=col_w, repeatRows=1)
    stripe = colors.HexColor("#F8F7F5")
    tbl.setStyle(TableStyle([
        # Header
        ("BACKGROUND", (0, 0), (-1, 0), BLUE),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 7.5),
        ("TOPPADDING", (0, 0), (-1, 0), 6),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ("ALIGN", (0, 0), (-1, 0), "LEFT"),
        # Body
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 7.5),
        ("TEXTCOLOR", (0, 1), (-1, -1), INK2),
        ("TOPPADDING", (0, 1), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, stripe]),
        # Grid
        ("LINEBELOW", (0, 0), (-1, -2), 0.3, LINE),
        ("LINEBELOW", (0, -1), (-1, -1), 0.5, LINE),
        ("BOX", (0, 0), (-1, -1), 0.5, LINE),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(tbl)


def _analytics_section(story, monthly, countries, charges, efficiency, styles):
    story += _section_title("Analytics & Cost Breakdown", styles)

    # -- Monthly table
    story.append(Paragraph("Monthly Cost Summary", styles["h3"]))
    story.append(Spacer(1, 2 * mm))

    if monthly:
        m_hdr = ["Month", "Shipments", "Total Cost", "Avg Cost/KG"]
        m_cw = [45 * mm, 35 * mm, 45 * mm, 45 * mm]
        m_rows = [m_hdr] + [
            [m["month"], str(m["count"]), _fmt(m["total"]), _fmt(m["avg_per_kg"])]
            for m in monthly
        ]
        m_tbl = Table(m_rows, colWidths=m_cw, repeatRows=1)
        stripe = colors.HexColor("#F8F7F5")
        m_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EFF6FF")),
            ("TEXTCOLOR", (0, 0), (-1, 0), BLUE_DARK),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 8),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 8),
            ("TEXTCOLOR", (0, 1), (-1, -1), INK2),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, stripe]),
            ("LINEBELOW", (0, 0), (-1, -2), 0.3, LINE),
            ("BOX", (0, 0), (-1, -1), 0.5, LINE),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ]))
        story.append(m_tbl)
    else:
        story.append(Paragraph("No monthly data available.", styles["small"]))

    story.append(Spacer(1, 6 * mm))

    # -- Charge composition
    story.append(Paragraph("Charge Composition", styles["h3"]))
    story.append(Spacer(1, 2 * mm))

    if charges:
        keys = ["transport", "fuel", "remote", "duty", "other"]
        labels = ["Transport", "Fuel Surcharge", "Remote Area", "Duty / Tax", "Other"]
        accent = [BLUE, AMBER, PURPLE, RED, CYAN]
        total_ch = sum(charges.get(k) or 0 for k in keys)
        for k, lbl, col in zip(keys, labels, accent):
            v = charges.get(k) or 0
            pct = v / total_ch if total_ch else 0
            story.append(ProgressBar(lbl, _fmt(v) + f"  ({pct*100:.1f}%)", pct, col))
            story.append(Spacer(1, 1 * mm))

    story.append(Spacer(1, 6 * mm))

    # -- Destination breakdown
    story.append(Paragraph("Top Destinations by Spend", styles["h3"]))
    story.append(Spacer(1, 2 * mm))

    if countries:
        top = countries[:8]
        max_v = top[0]["total_cost"] if top else 1
        dest_colors = [BLUE, GREEN, PURPLE, CYAN, AMBER, RED,
                       colors.HexColor("#0891B2"), colors.HexColor("#7C3AED")]
        for i, row in enumerate(top):
            pct = (row["total_cost"] or 0) / max_v if max_v else 0
            col = dest_colors[i % len(dest_colors)]
            label = row["destination"] or "Unknown"
            val_str = _fmt(row["total_cost"]) + f"  ·  {row['count']} shipments"
            story.append(ProgressBar(label, val_str, pct, col))
            story.append(Spacer(1, 1 * mm))

    story.append(Spacer(1, 6 * mm))

    # -- Efficiency report
    story.append(Paragraph("Cost Efficiency — Best & Worst Routes ($/kg)", styles["h3"]))
    story.append(Spacer(1, 2 * mm))

    if efficiency.get("worst") or efficiency.get("best"):
        eff_hdr = ["Tracking ID", "Destination", "Date", "Weight", "Total", "$/kg", "Rating"]
        eff_cw = [36 * mm, 30 * mm, 22 * mm, 18 * mm, 20 * mm, 16 * mm, 28 * mm]
        eff_rows = [eff_hdr]
        for row in (efficiency.get("worst") or []):
            eff_rows.append([
                Paragraph(row.get("tracking_id") or "—",
                          ParagraphStyle("m2", fontName="Courier", fontSize=7, textColor=INK)),
                row.get("destination") or "—",
                _fmtdate(row.get("ship_date")),
                _fmtkg(row.get("gross_weight")),
                _fmt(row.get("total_cost")),
                _fmt(row.get("cost_per_kg")),
                Paragraph("▲ Expensive", ParagraphStyle("r", fontName="Helvetica-Bold",
                                                        fontSize=7, textColor=RED)),
            ])
        for row in (efficiency.get("best") or []):
            eff_rows.append([
                Paragraph(row.get("tracking_id") or "—",
                          ParagraphStyle("m3", fontName="Courier", fontSize=7, textColor=INK)),
                row.get("destination") or "—",
                _fmtdate(row.get("ship_date")),
                _fmtkg(row.get("gross_weight")),
                _fmt(row.get("total_cost")),
                _fmt(row.get("cost_per_kg")),
                Paragraph("▼ Efficient", ParagraphStyle("r2", fontName="Helvetica-Bold",
                                                        fontSize=7, textColor=GREEN)),
            ])

        eff_tbl = Table(eff_rows, colWidths=eff_cw, repeatRows=1)
        stripe = colors.HexColor("#F8F7F5")
        eff_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EFF6FF")),
            ("TEXTCOLOR", (0, 0), (-1, 0), BLUE_DARK),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 7.5),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 7.5),
            ("TEXTCOLOR", (0, 1), (-1, -1), INK2),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, stripe]),
            ("LINEBELOW", (0, 0), (-1, -2), 0.3, LINE),
            ("BOX", (0, 0), (-1, -1), 0.5, LINE),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(eff_tbl)


def _alerts_section(story, alerts, styles):
    if not alerts:
        return
    story += _section_title("System Alerts", styles)
    story.append(Spacer(1, 2 * mm))

    type_meta = {
        "error":   (RED,   RED_LIGHT,   "✕  Error"),
        "warning": (AMBER, AMBER_LIGHT, "⚠  Warning"),
        "info":    (BLUE,  BLUE_LIGHT,  "ℹ  Info"),
    }
    for a in alerts:
        t = a.get("type", "info")
        col, bg, badge = type_meta.get(t, (BLUE, BLUE_LIGHT, "ℹ  Info"))
        row = [[
            Paragraph(f'<font color="white"><b>{badge}</b></font>',
                      ParagraphStyle("ab", fontName="Helvetica-Bold", fontSize=7.5,
                                     textColor=WHITE)),
            Paragraph(a.get("message", ""), styles["body"]),
        ]]
        tbl = Table(row, colWidths=[24 * mm, PAGE_W - 2 * MARGIN - 24 * mm])
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (0, 0), col),
            ("BACKGROUND", (1, 0), (1, 0), bg),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("BOX", (0, 0), (-1, -1), 0.5, LINE),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 2 * mm))


def _fuel_section(story, fuel_trend, styles):
    if not fuel_trend:
        return
    story += _section_title("Fuel Surcharge Trend", styles)
    story.append(Paragraph(
        "Monthly average fuel surcharge percentage and amount.", styles["body"]
    ))
    story.append(Spacer(1, 3 * mm))

    hdr = ["Month", "Avg Fuel %", "Avg Fuel Amount"]
    cw = [55 * mm, 55 * mm, 60 * mm]
    rows = [hdr] + [
        [f["month"], f"{f.get('fuel_pct') or 0:.1f}%", _fmt(f.get("avg_fuel_amt"))]
        for f in fuel_trend
    ]
    tbl = Table(rows, colWidths=cw, repeatRows=1)
    stripe = colors.HexColor("#FFFBEB")
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), AMBER_LIGHT),
        ("TEXTCOLOR", (0, 0), (-1, 0), AMBER),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("TEXTCOLOR", (0, 1), (-1, -1), INK2),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, stripe]),
        ("LINEBELOW", (0, 0), (-1, -2), 0.3, LINE),
        ("BOX", (0, 0), (-1, -1), 0.5, LINE),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
    ]))
    story.append(tbl)


def _footer_page(story, styles):
    story.append(PageBreak())
    story.append(Spacer(1, 40 * mm))
    story.append(ColorBar(height=3, color=BLUE))
    story.append(Spacer(1, 8 * mm))
    story.append(Paragraph("Nexus Shipping Intelligence", styles["h1"]))
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph(
        "This report is confidential and intended solely for the use of the named recipient. "
        "All data is derived from documents processed by the Nexus platform. "
        "Figures are indicative and should be verified against original invoices.",
        styles["body"],
    ))
    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph(
        f"Report generated: {datetime.now().strftime('%d %B %Y at %H:%M')}",
        styles["small"],
    ))
    story.append(Spacer(1, 2 * mm))
    story.append(Paragraph("nexus-shipping.local  ·  v2.0  ·  Fully local, no external APIs",
                            styles["small"]))


# ── Public API ────────────────────────────────────────────────────────────────

def generate_report(
    stats: dict,
    shipments: list,
    monthly: list,
    countries: list,
    charges: dict,
    fuel_trend: list,
    efficiency: dict,
    alerts: list,
    period_label: str = "All Time",
    title: str = "Shipping Intelligence Report",
) -> bytes:
    """Return a PDF as bytes."""
    buf = io.BytesIO()
    doc = _build_doc(buf, title)
    s = _styles()
    story = []

    # Cover page
    story.append(NextPageTemplate("content"))
    _cover(story, stats, period_label, s)
    story.append(PageBreak())

    # 1. Shipment Register
    if shipments:
        _shipments_section(story, shipments, s)
        story.append(PageBreak())

    # 2. Analytics
    _analytics_section(story, monthly, countries, charges, efficiency, s)
    story.append(PageBreak())

    # 3. Fuel Trend
    _fuel_section(story, fuel_trend, s)
    story.append(Spacer(1, 6 * mm))

    # 4. Alerts
    _alerts_section(story, alerts, s)

    # 5. Closing page
    _footer_page(story, s)

    doc.build(story)
    return buf.getvalue()
