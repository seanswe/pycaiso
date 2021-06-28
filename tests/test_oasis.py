import time
from datetime import datetime, timedelta

import pandas as pd
import pytest
from freezegun import freeze_time
from pycaiso.oasis import Atlas, BadDateRangeError, Node, Oasis, SystemDemand, get_lmps


@pytest.fixture()
def lmp_df_columns():
    return [
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


@pytest.fixture(scope="session", autouse=True)
def frozen_time():
    with freeze_time("2020-01-02"):
        yield


@pytest.fixture()
def node_lmps_default_df(scope="session"):
    """
    Basic API call to get LMPs in DAM for period 2020-01-1 to 2020-01-02 as df usng Node method
    """

    cj = Node("CAPTJACK_5_N003")
    df = cj.get_lmps(datetime(2020, 1, 1), datetime(2020, 1, 2))

    time.sleep(5)  # TODO: add rate limiter

    return df


@pytest.fixture()
def get_lmps_default_df(scope="session"):
    """
    Basic API call to get LMPs in DAM for period 2020-01-1 to 2020-01-02 as df using get_lmps func
    """

    df = get_lmps("CAPTJACK_5_N003", datetime(2020, 1, 1), datetime(2020, 1, 2))

    time.sleep(5)  # TODO: add rate limiter

    return df


@pytest.fixture()
def node_lmps_rtm_df(scope="session"):
    """
    Basic API call to get LMPs in DAM for period 2020-01-1 to 2020-01-02 as df
    """

    cj = Node("CAPTJACK_5_N003")
    df = cj.get_lmps(datetime(2020, 1, 1), datetime(2020, 1, 2), market="RTM")

    time.sleep(5)  # TODO: add rate limiter

    return df


def test_get_lmps_same(node_lmps_default_df, get_lmps_default_df):
    """
    test if Node get_lmps method returns same df as get_lmps func
    """

    assert node_lmps_default_df.equals(get_lmps_default_df)


def test_node_get_lmps_is_df(node_lmps_default_df):
    """
    test if basic API call returns pandas DataFrame
    """

    assert isinstance(node_lmps_default_df, pd.DataFrame)


def test_get_lmps_default_is_df(get_lmps_default_df):
    """
    test if basic API call returns pandas DataFrame
    """

    assert isinstance(get_lmps_default_df, pd.DataFrame)


def test_get_lmps_default_df(get_lmps_default_df, lmp_df_columns):
    """
    test for correct columns for get_lmps_default_df
    """

    assert sorted(get_lmps_default_df.columns) == sorted(lmp_df_columns)


def test_node_get_lmps_default_is_dam(node_lmps_default_df):
    """
    test if basic API call returns results from DAM by default
    """

    assert node_lmps_default_df.MARKET_RUN_ID.unique() == ["DAM"]


def test_node_get_lmps_default_is_rtm(node_lmps_rtm_df):
    """
    test if basic API call returns RTM when specified
    """

    assert node_lmps_rtm_df.MARKET_RUN_ID.unique() == ["RTM"]


@pytest.fixture(scope="session")
def demand_forecast_df():
    """
    Basic API call to get demand forecast
    """

    df = SystemDemand().get_demand_forecast(datetime(2020, 1, 1), datetime(2020, 1, 3))

    time.sleep(5)

    return df


def test_demand_forecast_is_df(demand_forecast_df):
    """
    Test if demand forecast API call returns pandas DataFrame
    """

    assert isinstance(demand_forecast_df, pd.DataFrame)


@pytest.fixture(scope="session")
def atlas_df():
    """
    Basic API call to get list of all pnodes
    """

    atl = Atlas()
    df = atl.get_pnodes(datetime(2021, 1, 1), datetime(2021, 2, 1))

    time.sleep(5)

    return df


@pytest.mark.parametrize(
    "start, end",
    [
        (datetime(2021, 1, 2), datetime(2021, 1, 1)),  # start before end
        (datetime.now() + timedelta(100), datetime.now()),  # start after today
        (datetime.now(), datetime.now() + timedelta(100)),  # end after today
        (datetime(2021, 1, 2), datetime(2021, 1, 2)),  # start == end
    ],
)
def test_validate_date_range_start_after_end(start, end):
    """
    Test if validate_date_range handles start date after end date
    """

    with pytest.raises(BadDateRangeError):
        Oasis._validate_date_range(start, end)
