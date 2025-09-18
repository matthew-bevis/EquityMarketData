def common_event(trade_dt=None, rec_type=None, symbol=None, exchange=None,
                 event_tm=None, event_seq_nb=None, arrival_tm=None,
                 trade_pr=None, bid_pr=None, bid_size=None,
                 ask_pr=None, ask_size=None,
                 partition="B", raw_line=None):
    """
    Returns a tuple matching common_event_schema
    """
    return (
        trade_dt, rec_type, symbol, exchange,
        event_tm, event_seq_nb, arrival_tm,
        trade_pr, bid_pr, bid_size,
        ask_pr, ask_size,
        partition
    )
