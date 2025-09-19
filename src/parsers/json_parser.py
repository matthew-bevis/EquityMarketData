from decimal import Decimal, InvalidOperation
from datetime import datetime
import json
from src.common_event import common_event


def parse_decimal(value, default="0.0"):
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def parse_timestamp(ts: str):
    if not ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts.strip(), fmt)
        except (ValueError, AttributeError):
            continue
    return None


def parse_json(line: str):
    try:
        record = json.loads(line)
        rec_type = record.get("event_type", "B").strip()

        if rec_type == "T":
            required_fields = ["trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr"]
            if not all(f in record for f in required_fields):
                raise ValueError("Missing fields in trade record")

            return common_event(
                trade_dt=record.get("trade_dt", "").strip(),
                rec_type="T",
                symbol=record.get("symbol", "").strip(),
                exchange=record.get("exchange", "").strip(),
                event_tm=parse_timestamp(record.get("event_tm")),
                event_seq_nb=int(record.get("event_seq_nb", 0)),
                arrival_tm=parse_timestamp(record.get("arrival_tm")),
                trade_pr=parse_decimal(record.get("trade_pr", 0.0)),
                partition="T"
            )

        elif rec_type == "Q":
            required_fields = ["trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb",
                               "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size"]
            if not all(f in record for f in required_fields):
                raise ValueError("Missing fields in quote record")

            return common_event(
                trade_dt=record.get("trade_dt", "").strip(),
                rec_type="Q",
                symbol=record.get("symbol", "").strip(),
                exchange=record.get("exchange", "").strip(),
                event_tm=parse_timestamp(record.get("event_tm")),
                event_seq_nb=int(record.get("event_seq_nb", 0)),
                arrival_tm=parse_timestamp(record.get("arrival_tm")),
                bid_pr=parse_decimal(record.get("bid_pr", 0.0)),
                bid_size=int(record.get("bid_size", 0)),
                ask_pr=parse_decimal(record.get("ask_pr", 0.0)),
                ask_size=int(record.get("ask_size", 0)),
                partition="Q"
            )

        else:
            raise ValueError("Unknown event_type")

    except Exception:
        return common_event(partition="B", raw_line=line)


