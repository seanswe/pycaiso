import io
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests
from dateutil.relativedelta import relativedelta


def get_lmp_zip(
    year, month, market="DAM", node="DLAP_SCE-APND", tz="America/Los_Angeles"
):
    url = "http://oasis.caiso.com/oasisapi/SingleZip?"

    start = datetime(year, month, 1)
    end = start + relativedelta(months=1)

    tz_ = pytz.timezone(tz)
    fmt = "%Y%m%dT%H:%M-0000"
    start_str = tz_.localize(start).astimezone(pytz.UTC).strftime(fmt)
    end_str = tz_.localize(end).astimezone(pytz.UTC).strftime(fmt)

    market_dict = {"DAM": "PRC_LMP", "RTM": "PRC_INTVL_LMP", "RTPD": "PRC_RTPD_LMP"}

    params = {
        "market_run_id": market,
        "queryname": market_dict[market],
        "startdatetime": start_str,
        "enddatetime": end_str,
        "version": 1,
        "node": node,
        "resultformat": 6,
    }

    str_params = "&".join(f"{k}={v}" for k, v in params.items())

    r = requests.get(url, params=str_params)

    try:
        z = zipfile.ZipFile(io.BytesIO(r.content))
    except:
        raise zipfile.BadZipFile("Invalid zip file")

    csv = z.open(z.namelist()[0])

    return pd.read_csv(csv, parse_dates=[2])