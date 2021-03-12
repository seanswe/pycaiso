import io
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests
from dateutil.relativedelta import relativedelta


class MarketNode:
    def __init__(self, market="DAM", node="DLAP_SCE-APND", tz="America/Los_Angeles"):
        self.market = market
        self.node = node
        self.tz = tz
        self.market_mapping = {
            "DAM": "PRC_LMP",
            "RTM": "PRC_INTVL_LMP",
            "RTPD": "PRC_RTPD_LMP",
        }

    def __repr__(self):
        return f"MarketNode(market='{self.market}', node='{self.node}', tz='{self.tz}')"

    def get_lmps(self, start, end):
        """Gets Locational Market Prices (LMPs) for a given pair of start and end dates

        Parameters:

        start (datetime.datetime): start date
        end (datetime.datetime): end date

        Returns:

        (pandas.DataFrame): Pandas dataframe containing the LMPs for given period
        """

        url = "http://oasis.caiso.com/oasisapi/SingleZip?"

        tz_ = pytz.timezone(self.tz)
        fmt = "%Y%m%dT%H:%M-0000"
        start_str = tz_.localize(start).astimezone(pytz.UTC).strftime(fmt)
        end_str = tz_.localize(end).astimezone(pytz.UTC).strftime(fmt)

        params = {
            "market_run_id": self.market,
            "queryname": self.market_mapping.get(self.market, "PRC_LMP"),
            "startdatetime": start_str,
            "enddatetime": end_str,
            "version": 1,
            "node": self.node,
            "resultformat": 6,
        }

        str_params = "&".join(f"{k}={v}" for k, v in params.items())

        r = requests.get(url, params=str_params)

        try:
            z = zipfile.ZipFile(io.BytesIO(r.content))
        except:
            raise zipfile.BadZipFile("Invalid zip file")

        csv = z.open(z.namelist()[0])

        df = (
            pd.read_csv(csv, parse_dates=[2])
            .sort_values(["OPR_DT", "OPR_HR"])
            .reset_index(drop=True)
        )

        return df

    def get_month_lmps(self, year, month):
        """Helper method to get LMPs for a complete month

        Parameters:

        year(int): year of LMPs desired
        month(int): month of LMPs desired

        Returns:

        (pandas.DataFrame): Pandas dataframe containing the LMPs for given month
        """

        start = datetime(year, month, 1)
        end = start + relativedelta(months=1)

        return self.get_lmps(start, end)
