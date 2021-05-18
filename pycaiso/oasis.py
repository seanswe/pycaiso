import io
import zipfile
from datetime import datetime, timedelta
import re

import pandas as pd
import pytz
import requests
from dateutil.relativedelta import relativedelta


class Oasis:
    def __init__(self):
        self.base_url = "http://oasis.caiso.com/oasisapi/SingleZip?"

    @staticmethod
    def validate_date_range(start, end):
        error = None

        if start > end:
            error = "Start must be before end."

        if end > datetime.now().date():
            error = "End must be before or equal to today."

        if start.date() == end.date():
            error = "Start and end cannot be equal. To return data only from start date, pass only start."

        if error is not None:
            raise BadDateRangeError(error)

    def request(self, params):
        """Make http request

        Base method to get request at base_url

        Args:
            params (dict): keyword params to construct request

        Returns:
            response: requests response object
        """

        response = requests.get(self.base_url, params=params, timeout=10)
        response.raise_for_status()

        headers = response.headers["content-disposition"]

        if re.search(r"\.xml\.zip;$", headers):
            raise NoDataAvailableError("No data available for this query.")

        return response

    def get_UTC_string(
        self, dt, local_tz="America/Los_Angeles", fmt="%Y%m%dT%H:%M-0000"
    ):
        """Convert local datetime to UTC string

        Converts datetime.datetime or pandas.Timestamp in local time to
        to UTC string for constructing HTTP request

        Args:
            dt (datetime.datetime): datetime to convert
            local_tz (str): timezone

        Returns:
            utc (str): UTC string
        """

        tz_ = pytz.timezone(local_tz)
        return tz_.localize(dt).astimezone(pytz.UTC).strftime(fmt)

    def get_df(self, response, parse_dates=False, sort_values=None):

        """Convert response to datframe

        Converts requests.response to pandas.DataFrame

        Args:
            r : requests response object
            parse_dates (bool, list): which columns to parse dates if any
            sort_values(list): which columsn to sort by if any

        Returns:
            df (pandas.DataFrame): pandas dataframe
        """

        COLUMNS = [
            "INTERVALSTARTTIME_GMT",
            "INTERVALENDTIME_GMT",
            "OPR_DT",
            "OPR_HR",
            "OPR_INTERVAL",
            "NODE_ID_XML",
            "NODE_ID",
            "NODE",
            "MARKET_RUN_ID",
            "LMP_TYPE",
            "XML_DATA_ITEM",
            "PNODE_RESMRID",
            "GRP_TYPE",
            "POS",
            "MW",
            "GROUP",
        ]

        with io.BytesIO() as buffer:
            try:
                buffer.write(response.content)
                buffer.seek(0)
                z = zipfile.ZipFile(buffer)

            except zipfile.BadZipFile as e:
                print("Bad zip file", e)

            else:
                csv = z.open(z.namelist()[0])  # ignores all but first file in zip
                df = pd.read_csv(csv, parse_dates=parse_dates)

                if sort_values:
                    df = df.sort_values(sort_values).reset_index(drop=True)

        return df.reindex(columns=COLUMNS)


class Node(Oasis):
    """CAISO PNode"""

    def __init__(self, node):
        self.node = node
        super().__init__()

    def __repr__(self):
        return f"Node(node='{self.node}')"

    def get_lmps(self, start, end=None, market="DAM"):
        """Get LMPs

        Gets Locational Market Prices (LMPs) for a given pair of start and end dates

        Args:
            start (datetime.datetime): start date, inclusive
            end (datetime.datetime): end date, exclusive
            market (str): market for prices; must be "DAM", "RTM", or "RTPD"

        Returns:
            (pandas.DataFrame): Pandas dataframe containing the LMPs for given period, market
        """

        if end is None:
            end = start + timedelta(days=1)

        self.validate_date_range(start, end)

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

        response = self.request(params)

        return self.get_df(response, parse_dates=[2], sort_values=["OPR_DT", "OPR_HR"])

    def get_month_lmps(self, year, month):

        """Get LMPs for entire month

        Helper method to get LMPs for a complete month

        Args:
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


class Atlas(Oasis):
    """Atlas data"""

    def __init__(self):
        super().__init__()

    def get_pnodes(self, start, end):

        """Get pricing nodes

        Get lost of pricing nodes and aggregated pricing nodes extant between
        start and stop period

        Args:
            start (datetime.datetime): start date
            end (datetime.datetime): end date

        Returns:
            (pandas.DataFrame): List of pricing nodes
        """

        self.validate_date_range(start, end)

        params = {
            "queryname": "ATL_PNODE",
            "startdatetime": self.get_UTC_string(start),
            "enddatetime": self.get_UTC_string(end),
            "Pnode_type": "ALL",
            "version": 1,
            "resultformat": 6,
        }

        response = self.request(params)

        return self.get_df(response)


class SystemDemand(Oasis):
    """System Demand"""

    def __init__(self):
        super().__init__()

    def get_peak_demand_forecast(self, start, end):
        """Get peak demand forecast

        Get peak demand forecasted between start and end dates

        Args:
            start (datetime.datetime): start date
            end (datetime.datetime): end date

        Returns:
            (pandas.DataFrame): peak demand forecast
        """

        params = {
            "queryname": "SLD_FCST_PEAK",
            "startdatetime": self.get_UTC_string(start),
            "enddatetime": self.get_UTC_string(end),
            "version": 1,
            "resultformat": 6,
        }

        response = self.request(params)

        return self.get_df(response)

    def get_demand_forecast(self, start, end):

        """Get demand forecast

        Get demand forecasted between start and end dates

        Args:
            start (datetime.datetime): start date
            end (datetime.datetime): end date

        Returns:
            (pandas.DataFrame): demand forecast
        """

        params = {
            "queryname": "SLD_FCST",
            "startdatetime": self.get_UTC_string(start),
            "enddatetime": self.get_UTC_string(end),
            "version": 1,
            "resultformat": 6,
        }

        response = self.request(params)

        return self.get_df(response)


class NoDataAvailableError(Exception):
    pass


class BadDateRangeError(Exception):
    pass
