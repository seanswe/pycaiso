import io
import zipfile
from datetime import datetime, timedelta
import re

import pandas as pd
import pytz
import requests
from dateutil.relativedelta import relativedelta


class RequestMixIn:
    """Base class for OASIS API calls"""

    def getRequestStr(
        self,
        start,
        end,
        tz,
        node=None,
        market=None,
        url="http://oasis.caiso.com/oasisapi/SingleZip?",
    ):
        """
        helper function to assemble request string
        """

        query_mapping = {
            "DAM": "PRC_LMP",
            "RTM": "PRC_INTVL_LMP",
            "RTPD": "PRC_RTPD_LMP",
        }

        if market is not None:
            if market not in query_mapping.keys():
                raise ValueError("market must be 'DAM', 'RTM' or 'RTPD'")

        tz_ = pytz.timezone(tz)
        fmt = "%Y%m%dT%H:%M-0000"
        start_str = tz_.localize(start).astimezone(pytz.UTC).strftime(fmt)
        end_str = tz_.localize(end).astimezone(pytz.UTC).strftime(fmt)

        params = {
            "queryname": query_mapping[market],
            "market_run_id": market,
            "startdatetime": start_str,
            "enddatetime": end_str,
            "version": 1,
            "node": node,
            "resultformat": 6,
        }

        str_params = "&".join(f"{k}={v}" for k, v in params.items())

        return url + str_params

    def getRequest(
        self,
        request_str,
    ):
        """
        helper function to get http request and handle exceptions
        """

        try:
            r = requests.get(request_str, timeout=5)
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

        return r


class Node(RequestMixIn):
    """CAISO PNode"""

    def __init__(self, node):
        self.node = node

    def __repr__(self):
        return f"Node(node='{self.node}')"

    def get_lmps(
        self,
        start,
        end,
        tz="America/Los_Angeles",
        market="DAM",
    ):
        """Gets Locational Market Prices (LMPs) for a given pair of start and end dates

        Parameters:

        start (datetime.datetime): start date
        end (datetime.datetime): end date
        market (str): market for prices; must be "DAM", "RTM", or "RTPD"

        Returns:

        (pandas.DataFrame): Pandas dataframe containing the LMPs for given period, market
        """

        request_str = self.getRequestStr(
            start, end, tz=tz, node=self.node, market=market
        )

        r = self.getRequest(request_str)

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


# class Atlas(RequestMixIn):
#     """Atlas data """

#     def get_pnodes(
#         self,
#         start,
#         end,
#         tz,
#     ):

#         request_str = self.getRequestStr(start, end, tz=tz)

#         r = self.getRequestStr(request_str)
