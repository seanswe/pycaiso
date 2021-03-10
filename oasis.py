import io
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests
from dateutil.relativedelta import relativedelta


def get_lmp_zip(year, month, market="DAM", node="DLAP_SCE-APND"):

    fmt = "%Y%m%dT%H:%M-0000"

    url = "http://oasis.caiso.com/oasisapi/SingleZip?"

    start = datetime(year, month, 1)
    end = start + relativedelta(months=1)
    start = pacific.localize(start).astimezone(utc).strftime(fmt)
    end = pacific.localize(end).astimezone(utc).strftime(fmt)
    # start = start + "-0000"
    # end = end + "-0000"
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

    return str_params


print(get_lmp_zip(2020, 10))


# for m in range(1, 3):
#     try:
#         get_rt_month(2019, m)
#     except zipfile.BadZipFile as e:
#         print(e)
#         pass