import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def clean_text(text: str) -> str:
    """Standardizes whitespace and removes newlines."""
    if not text:
        return ""
    return " ".join(str(text).split()).strip()

def parse_relative_date(text: str, timezone: str = "America/Vancouver") -> str:
    """Converts relative time (e.g., '3 days ago', '2 weeks') to YYYY-MM-DD."""
    if not text or "just" in text.lower() or text.lower() == "no date":
        return datetime.now(ZoneInfo(timezone)).strftime('%Y-%m-%d')

    text = text.lower().strip()
    now = datetime.now(ZoneInfo(timezone))

    match = re.search(r'(\d+)', text)
    value = int(match.group(1)) if match else 0

    if any(k in text for k in ['hour', 'min', 'h', 'hr']):
        delta = timedelta(hours=0)
    elif any(k in text for k in ['day', 'd']):
        delta = timedelta(days=value)
    elif any(k in text for k in ['week', 'wk', 'w']):
        delta = timedelta(weeks=value)
    elif any(k in text for k in ['month', 'mo']):
        delta = timedelta(days=value * 30)
    else:
        delta = timedelta(0)

    return (now - delta).strftime('%Y-%m-%d')