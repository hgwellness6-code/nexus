import fitz  # PyMuPDF
import pdfplumber
import pytesseract
from PIL import Image
import io
import re
import os

# On Windows, set Tesseract path if needed
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def extract_text_from_pdf(filepath: str) -> dict:
    """
    Extract text from PDF using best available method.
    Returns dict with text, method used, and page count.
    """
    result = {"text": "", "method": "none", "pages": 0, "tables": []}

    # Try PyMuPDF first (fastest for digital PDFs)
    try:
        doc = fitz.open(filepath)
        result["pages"] = len(doc)
        all_text = []
        for page in doc:
            text = page.get_text("text")
            if text.strip():
                all_text.append(text)
        doc.close()

        combined = "\n".join(all_text)
        if len(combined.strip()) > 100:
            result["text"] = combined
            result["method"] = "pymupdf"
            return result
    except Exception as e:
        print(f"[PyMuPDF] Error: {e}")

    # Try pdfplumber for table-heavy PDFs
    try:
        with pdfplumber.open(filepath) as pdf:
            result["pages"] = len(pdf.pages)
            all_text = []
            all_tables = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    all_text.append(text)
                tables = page.extract_tables()
                if tables:
                    all_tables.extend(tables)

            combined = "\n".join(all_text)
            if len(combined.strip()) > 100:
                result["text"] = combined
                result["method"] = "pdfplumber"
                result["tables"] = all_tables
                return result
    except Exception as e:
        print(f"[pdfplumber] Error: {e}")

    # Fallback: OCR with Tesseract (for scanned PDFs)
    try:
        doc = fitz.open(filepath)
        result["pages"] = len(doc)
        all_text = []
        for page in doc:
            mat = fitz.Matrix(2.0, 2.0)  # 2x zoom for better OCR
            pix = page.get_pixmap(matrix=mat)
            img_bytes = pix.tobytes("png")
            img = Image.open(io.BytesIO(img_bytes))
            text = pytesseract.image_to_string(img, config='--psm 6')
            all_text.append(text)
        doc.close()
        result["text"] = "\n".join(all_text)
        result["method"] = "tesseract_ocr"
        return result
    except Exception as e:
        print(f"[Tesseract] Error: {e}")

    return result


def extract_text_from_image(filepath: str) -> dict:
    """Extract text from image files using Tesseract."""
    try:
        img = Image.open(filepath)
        text = pytesseract.image_to_string(img, config='--psm 6')
        return {"text": text, "method": "tesseract_image", "pages": 1, "tables": []}
    except Exception as e:
        print(f"[Image OCR] Error: {e}")
        return {"text": "", "method": "error", "pages": 0, "tables": []}


def detect_doc_type(text: str, filename: str) -> str:
    """Auto-detect document type from text content."""
    text_lower = text.lower()
    filename_lower = filename.lower()

    ups_keywords = ['ups', 'united parcel', 'fuel surcharge', 'transportation charge',
                    'remote area', 'brokerage', 'ups invoice', 'service charge']
    export_keywords = ['export invoice', 'commercial invoice', 'proforma', 'airway bill',
                       'awb', 'consignee', 'shipper', 'country of origin', 'hs code']
    pod_keywords = ['proof of delivery', 'delivered', 'signature', 'pod', 'delivery confirmation']
    customs_keywords = ['customs', 'clearance', 'declaration', 'duty', 'tariff', 'import permit']

    ups_score = sum(1 for k in ups_keywords if k in text_lower)
    export_score = sum(1 for k in export_keywords if k in text_lower)
    pod_score = sum(1 for k in pod_keywords if k in text_lower)
    customs_score = sum(1 for k in customs_keywords if k in text_lower)

    if 'ups' in filename_lower and ups_score >= 1:
        return 'ups_invoice'
    if 'pod' in filename_lower or pod_score >= 2:
        return 'pod'
    if 'custom' in filename_lower or customs_score >= 2:
        return 'customs'

    scores = {'ups_invoice': ups_score, 'export_invoice': export_score,
              'pod': pod_score, 'customs': customs_score}
    best = max(scores, key=scores.get)
    if scores[best] == 0:
        return 'unknown'
    return best
