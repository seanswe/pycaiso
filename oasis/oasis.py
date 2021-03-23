import io
import zipfile
from datetime import datetime, timedelta
import re

import pandas as pd
import pytz
import requests
from dateutil.relativedelta import relativedelta


class RequestMixIn:
    """Mixin to make http request and handle exceptions"""

    def getRequest(self, url, params):
        """
        helper function to get http request and handle exceptions
        """

        try:
            r = requests.get(url, params=params, timeout=5)
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

    def get_UTC_string(
        self, ts, local_tz="America/Los_Angeles", fmt="%Y%m%dT%H:%M-0000"
    ):
        """
        convert local datetime.datetime to string-formatted UTC
        """

        tz_ = pytz.timezone(local_tz)
        return tz_.localize(ts).astimezone(pytz.UTC).strftime(fmt)


class DataFrameMixIn:
    """
    MixIn to convert http request results to pandas dataframe
    """

    def get_df(self, r, parse_dates=False, sort_values=None):
        with io.BytesIO() as buffer:
            try:
                buffer.write(r.content)
                buffer.seek(0)
                z = zipfile.ZipFile(buffer)

            except zipfile.BadZipFile as e:
                print("Bad zip file", e)

            else:
                csv = z.open(z.namelist()[0])
                df = pd.read_csv(csv, parse_dates=parse_dates)

                if sort_values:
                    df = df.sort_values(sort_values).reset_index(drop=True)

        return df


class Node(RequestMixIn, DataFrameMixIn):
    """CAISO PNode"""

    def __init__(self, node):
        self.node = node
        self._url = "http://oasis.caiso.com/oasisapi/SingleZip?"

    def __repr__(self):
        return f"Node(node='{self.node}')"

    def get_lmps(self, start, end, market="DAM"):
        """Gets Locational Market Prices (LMPs) for a given pair of start and end dates

        Parameters:

        start (datetime.datetime): start date
        end (datetime.datetime): end date
        market (str): market for prices; must be "DAM", "RTM", or "RTPD"

        Returns:

        (pandas.DataFrame): Pandas dataframe containing the LMPs for given period, market
        """

        query_mapping = {
            "DAM": "PRC_LMP",
            "RTM": "PRC_INTVL_LMP",
            "RTPD": "PRC_RTPD_LMP",
        }

        if market not in query_mapping.keys():
            raise ValueError("market must be 'DAM', 'RTM' or 'RTPD'")

        params = {
            "queryname": query_mapping[market],
            "market_run_id": market,
            "startdatetime": self.get_UTC_string(start),
            "enddatetime": self.get_UTC_string(end),
            "version": 1,
            "node": self.node,
            "resultformat": 6,
        }

        r = self.getRequest(self._url, params)

        return self.get_df(r, parse_dates=[2], sort_values=["OPR_DT", "OPR_HR"])

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


class Atlas(RequestMixIn, DataFrameMixIn):
    """Atlas data """

    def __init__(self):
        self._url = "http://oasis.caiso.com/oasisapi/SingleZip?"

    def get_pnodes(self, start, end):

        params = {
            "queryname": "ATL_PNODE",
            "startdatetime": self.get_UTC_string(start),
            "enddatetime": self.get_UTC_string(end),
            "Pnode_type": "ALL",
            "version": 1,
            "resultformat": 6,
        }

        r = self.getRequest(self._url, params)

        return self.get_df(r)
