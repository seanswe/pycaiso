from oasis.oasis import Node, Atlas, SystemDemand
from datetime import datetime
import pandas as pd
import pytz
import pytest


@pytest.fixture(scope="module")
def node_lmps_default_df():
    """
    Basic API call to get LMPs in DAM for period 2020-01-1 to 2020-01-02 as df
    """

    cj = Node("CAPTJACK_5_N003")
    df = cj.get_lmps(datetime(2020, 1, 1), datetime(2020, 1, 2))

    return df


@pytest.fixture(scope="module")
def node_lmps_rtm_df():
    """
    Basic API call to get LMPs in DAM for period 2020-01-1 to 2020-01-02 as df
    """

    cj = Node("CAPTJACK_5_N003")
    df = cj.get_lmps(datetime(2020, 1, 1), datetime(2020, 1, 2), market="RTM")

    return df


def test_node_get_lmps_is_df(node_lmps_default_df):
    """
    test if basic API call returns pandas DataFrame
    """

    assert isinstance(node_lmps_default_df, pd.DataFrame)


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


@pytest.fixture(scope="module")
def demand_forecast_df():
    """
    Basic API call to get demand forecast
    """

    df = SystemDemand().get_demand_forecast(datetime(2020, 1, 1), datetime(2020, 1, 2))

    return df


def test_demand_forecast_is_df(demand_forecast_df):
    """
    Test if demand forecast API call returns pandas DataFrame
    """

    assert isinstance(demand_forecast_df, pd.DataFrame)
