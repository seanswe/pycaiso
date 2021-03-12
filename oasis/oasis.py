import io
import zipfile
from datetime import datetime, timedelta
import re

import pandas as pd
import pytz
import requests
from dateutil.relativedelta import relativedelta


class Node:
    def __init__(self, node):
        self.node = node

    def __repr__(self):
        return f"MarketNode(node='{self.node}')"

    def get_lmps(self, start, end, market="DAM", tz="America/Los_Angeles"):
        """Gets Locational Market Prices (LMPs) for a given pair of start and end dates

        Parameters:

        start (datetime.datetime): start date
        end (datetime.datetime): end date
        market (str): market for prices; must be "DAM", "RTM", or "RTPD"

        Returns:

        (pandas.DataFrame): Pandas dataframe containing the LMPs for given period, market
        """

        market_mapping = {
            "DAM": "PRC_LMP",
            "RTM": "PRC_INTVL_LMP",
            "RTPD": "PRC_RTPD_LMP",
        }

        if market not in market_mapping.keys():
            raise ValueError("market must be 'DAM', 'RTM' or 'RTPD'")

        url = "http://oasis.caiso.com/oasisapi/SingleZip?"

        tz_ = pytz.timezone(tz)
        fmt = "%Y%m%dT%H:%M-0000"
        start_str = tz_.localize(start).astimezone(pytz.UTC).strftime(fmt)
        end_str = tz_.localize(end).astimezone(pytz.UTC).strftime(fmt)

        params = {
            "market_run_id": market,
            "queryname": market_mapping[market],
            "startdatetime": start_str,
            "enddatetime": end_str,
            "version": 1,
            "node": self.node,
            "resultformat": 6,
        }

        str_params = "&".join(f"{k}={v}" for k, v in params.items())

        # print(url + str_params)

        try:
            r = requests.get(url, params=str_params, timeout=10)
            r.raise_for_status()

        except requests.exceptions.HTTPError as eh:
            print("HTTP Error:", eh)

        except requests.exceptions.ConnectionError as ec:
            print("Connection Error:", ec)

        except requests.exceptions.Timeout as et:
            print("Timeout:", et)

        except requests.exceptions.RequestException as e:
            print(e)

        headers = r.headers["content-disposition"]
        if re.search(r"\.xml\.zip;$", headers):
            raise Exception("No data available for this query.")

        with io.BytesIO() as buffer:
            try:
                buffer.write(r.content)
                buffer.seek(0)
                z = zipfile.ZipFile(buffer)

            except zipfile.BadZipFile as e:
                print("Bad zip file", e)

            else:
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

    @classmethod
    def SP15(cls):
        return cls("TH_SP15_GEN-APND")

    @classmethod
    def NP15(cls):
        return cls("TH_NP15_GEN-APND")

    @classmethod
    def ZP26(cls):
        return cls("TH_ZP26_GEN-APND")

    @classmethod
    def SCEDLAP(cls):
        return cls("DLAP_SCE-APND")

    @classmethod
    def PGAEDLAP(cls):
        return cls("DLAP_PGAE-APND")

    @classmethod
    def SDGEDLAP(cls):
        return cls("DLAP_SDGE-APND")
