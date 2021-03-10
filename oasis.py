import io
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests
from dateutil.relativedelta import relativedelta


def get_lmp_zip(year, month, market="DAM", node="DLAP_SCE-APND"):

    fmt = "%Y%m%dT%H:%M-0000"

    pacific = pytz.timezone("US/Pacific")

    url = "http://oasis.caiso.com/oasisapi/SingleZip?"

    start = datetime(year, month, 1)
    end = start + relativedelta(months=1)
    start = pacific.localize(start).astimezone(pytz.UTC).strftime(fmt)
    end = pacific.localize(end).astimezone(pytz.UTC).strftime(fmt)
    market_dict = {"DAM": "PRC_LMP", "RTM": "PRC_INTVL_LMP", "RTPD": "PRC_RTPD_LMP"}
    params = {
        "market_run_id": market,
        "queryname": market_dict[market],
        "startdatetime": start,
        "enddatetime": end,
        "version": 1,
        "node": node,
        "resultformat": 6,
    }

    str_params = "&".join("%s=%s" % (k, v) for k, v in params.items())

    r = requests.get(url, params=str_params)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv = z.open(z.namelist()[0])
    return pd.read_csv(csv, parse_dates=[2])


df = get_lmp_zip(2021, 1)

print(df.head())


# for m in range(1, 3):
#     try:
#         get_rt_month(2019, m)
#     except zipfile.BadZipFile as e:
#         print(e)
#         pass