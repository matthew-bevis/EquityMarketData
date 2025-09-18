from decimal import Decimal, InvalidOperation
from datetime import datetime
import json
from src.common_event import common_event


def parse_decimal(value, default="0.0"):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def parse_timestamp(ts: str):
    if not ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    return None


def parse_json(line: str):
    try:
        record = json.loads(line)
        rec_type = record.get("event_type", "B")

        if rec_type == "T":
            return common_event(
                trade_dt=record.get("trade_dt"),
                rec_type="T",
                symbol=record.get("symbol"),
                exchange=record.get("exchange"),
                event_tm=parse_timestamp(record.get("event_tm")),
                event_seq_nb=int(record.get("event_seq_nb", 0)),
                arrival_tm=parse_timestamp(record.get("arrival_tm")),
                trade_pr=parse_decimal(record.get("trade_pr", 0.0)),
                partition="T"
            )
        elif rec_type == "Q":
            return common_event(
                trade_dt=record.get("trade_dt"),
                rec_type="Q",
                symbol=record.get("symbol"),
                exchange=record.get("exchange"),
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
            return common_event(partition="B", raw_line=line)
    except Exception:
        return common_event(partition="B", raw_line=line)

