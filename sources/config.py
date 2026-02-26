import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # --- app ---
    APP_NAME = os.getenv("APP_NAME", "TradingHub")
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "8000"))

    # --- trading ---
    PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
    DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "AAPL")
    DEFAULT_QTY = float(os.getenv("DEFAULT_QTY", "1"))

    # News lockout window: block trading within +/- this many minutes of high impact USD events
    NEWS_LOCKOUT_MINUTES = int(os.getenv("NEWS_LOCKOUT_MINUTES", "10"))

    # --- tradingview webhook ---
    # Set this to a long random string and require it in webhook payloads
    TV_WEBHOOK_SECRET = os.getenv("TV_WEBHOOK_SECRET", "change-me")

    # --- ForexFactory export url ---
    FF_EXPORT_URL = os.getenv(
        "FF_EXPORT_URL",
        "https://nfs.faireconomy.media/ff_calendar_thisweek.csv"
    )

    # --- tradovate placeholders (fill these in) ---
    TRADOVATE_BASE_URL = os.getenv("TRADOVATE_BASE_URL", "https://demo-api.tradovate.com/v1")
    TRADOVATE_USERNAME = os.getenv("TRADOVATE_USERNAME", "")
    TRADOVATE_PASSWORD = os.getenv("TRADOVATE_PASSWORD", "")
    TRADOVATE_APP_ID = os.getenv("TRADOVATE_APP_ID", "")
    TRADOVATE_APP_VERSION = os.getenv("TRADOVATE_APP_VERSION", "1.0")
    TRADOVATE_DEVICE_ID = os.getenv("TRADOVATE_DEVICE_ID", "tradinghub")
    TRADOVATE_CID = os.getenv("TRADOVATE_CID", "")
    TRADOVATE_SEC = os.getenv("TRADOVATE_SEC", "")

settings = Settings()
