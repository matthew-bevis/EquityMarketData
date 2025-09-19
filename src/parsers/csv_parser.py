from datetime import datetime
from decimal import Decimal, InvalidOperation
from src.common_event import common_event

def parse_timestamp(ts: str):
    if not ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts.strip(), fmt)
        except ValueError:
            continue
    return None

def parse_decimal(value, default="0.0"):
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)

def parse_csv(line: str):
    record_type_pos = 2
    record = line.split(",")

    try:
        if len(record) < 7:
            raise ValueError("Too few fields in line")

        rec_type = record[record_type_pos].strip()

        if rec_type == "T":
            if len(record) < 8:
                raise ValueError("Trade record missing fields")

            return common_event(
                trade_dt=record[0].strip(),
                rec_type="T",
                symbol=record[1].strip(),
                exchange=record[3].strip(),
                event_tm=parse_timestamp(record[4]),
                event_seq_nb=int(record[5]),
                arrival_tm=parse_timestamp(record[6]),
                trade_pr=parse_decimal(record[7]),
                partition="T"
            )

        elif rec_type == "Q":
            if len(record) < 11:
                raise ValueError("Quote record missing fields")

            return common_event(
                trade_dt=record[0].strip(),
                rec_type="Q",
                symbol=record[1].strip(),
                exchange=record[3].strip(),
                event_tm=parse_timestamp(record[4]),
                event_seq_nb=int(record[5]),
                arrival_tm=parse_timestamp(record[6]),
                bid_pr=parse_decimal(record[7]),
                bid_size=int(record[8]),
                ask_pr=parse_decimal(record[9]),
                ask_size=int(record[10]),
                partition="Q"
            )

        else:
            raise ValueError("Unknown record type")

    except Exception as e:
        # Log to bad records partition with raw line
        return common_event(partition="B", raw_line=line)

