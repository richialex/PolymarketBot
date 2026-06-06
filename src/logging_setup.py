from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path


LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


class DailyFileHandler(logging.Handler):
    def __init__(self, prefix: str, retention_days: int = 14) -> None:
        super().__init__(level=logging.INFO)
        self.prefix = prefix
        self.retention_days = retention_days
        self.current_date: date | None = None
        self.stream = None
        self.setFormatter(logging.Formatter(LOG_FORMAT))
        LOG_DIR.mkdir(exist_ok=True)
        self._cleanup_old_logs()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._ensure_stream()
            if self.stream is None:
                return
            self.stream.write(self.format(record) + "\n")
            self.flush()
        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        if self.stream is not None:
            self.stream.flush()

    def close(self) -> None:
        if self.stream is not None:
            self.stream.close()
            self.stream = None
        super().close()

    def _ensure_stream(self) -> None:
        today = date.today()
        if self.stream is not None and self.current_date == today:
            return
        if self.stream is not None:
            self.stream.close()
        self.current_date = today
        path = LOG_DIR / f"{self.prefix}-{today.isoformat()}.log"
        self.stream = path.open("a", encoding="utf-8")
        self._cleanup_old_logs()

    def _cleanup_old_logs(self) -> None:
        cutoff = datetime.now() - timedelta(days=self.retention_days)
        for path in LOG_DIR.glob(f"{self.prefix}-*.log"):
            try:
                if datetime.fromtimestamp(path.stat().st_mtime) < cutoff:
                    path.unlink()
            except OSError:
                pass


def configure_logging(retention_days: int = 14) -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    if not root.handlers:
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter(LOG_FORMAT))
        console._polymarket_console = True  # type: ignore[attr-defined]
        root.addHandler(console)

    if not any(getattr(h, "_polymarket_bot_file", False) for h in root.handlers):
        bot_file = DailyFileHandler("bot", retention_days=retention_days)
        bot_file._polymarket_bot_file = True  # type: ignore[attr-defined]
        root.addHandler(bot_file)


def configure_ws_logger(retention_days: int = 14) -> logging.Logger:
    logger = logging.getLogger("ws_probe")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not any(getattr(h, "_polymarket_ws_console", False) for h in logger.handlers):
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter(LOG_FORMAT))
        console._polymarket_ws_console = True  # type: ignore[attr-defined]
        logger.addHandler(console)

    if not any(getattr(h, "_polymarket_ws_file", False) for h in logger.handlers):
        ws_file = DailyFileHandler("ws", retention_days=retention_days)
        ws_file._polymarket_ws_file = True  # type: ignore[attr-defined]
        logger.addHandler(ws_file)

    return logger


def configure_ws_file_logger(name: str = "user_ws", retention_days: int = 14) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not any(getattr(h, "_polymarket_ws_file", False) for h in logger.handlers):
        ws_file = DailyFileHandler("ws", retention_days=retention_days)
        ws_file._polymarket_ws_file = True  # type: ignore[attr-defined]
        logger.addHandler(ws_file)

    return logger
