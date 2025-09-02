import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

BMRS_FUELINST_URL = "https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST/stream"
NESO_DAILY_DEMAND_URL = (
    "https://api.neso.energy/dataset/7a12172a-939c-404c-b581-a6128b74f588/"
    "resource/177f6fa4-ae49-4182-81ea-0c6b35f26ca6/download/demanddataupdate.csv"
)


def fetch_bmrs_fuelinst(hours=24):
    """
    Fetch BMRS FUELINST generation data for the past given number of
    hours.

    The data has 5 minute granularity - so for every settlement period
    of 30 mins there are 6x 5-minute averages for each fuel type's
    generation (MW).

    Args:
        hours (int): Number of hours of data to fetch.

    Returns:
        pd.DataFrame: BMRS fuel mix data with timestamps.
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)

    params = {
        "publishDateTimeFrom": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "publishDateTimeTo": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    resp = requests.get(BMRS_FUELINST_URL, params=params)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data)

    df["settlementDate"] = pd.to_datetime(df["settlementDate"], utc=True)
    df["timestamp"] = df["settlementDate"] + pd.to_timedelta(
        (df["settlementPeriod"] - 1) * 30, unit="m"
    )

    df = df[(df["timestamp"] >= start) & (df["timestamp"] <= now)]
    return df


def fetch_neso_demand(hours=24):
    """
    Fetch NESO embedded generation estimate for the past given number
    of hours.

    Generation (MW) is the average across a 30 minute time period.

    Args:
        hours (int): Number of hours of data to fetch.

    Returns:
        pd.DataFrame: NESO embedded generation data with timestamps.
    """
    df = pd.read_csv(NESO_DAILY_DEMAND_URL)

    df = df[
        [
            "SETTLEMENT_DATE",
            "SETTLEMENT_PERIOD",
            "EMBEDDED_WIND_GENERATION",
            "EMBEDDED_SOLAR_GENERATION",
        ]
    ]

    df["SETTLEMENT_DATE"] = pd.to_datetime(df["SETTLEMENT_DATE"], utc=True)

    df["timestamp"] = df["SETTLEMENT_DATE"] + pd.to_timedelta(
        (df["SETTLEMENT_PERIOD"] - 1) * 30, unit="m"
    )

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)
    df = df[(df["timestamp"] >= start) & (df["timestamp"] <= now)]
    return df


def merge_datasets(bmrs_fuelinst_df, neso_df):
    """
    Merge BMRS and NESO datasets by timestamp.

    Args:
        bmrs_fuelinst_df (pd.DataFrame): BMRS generation data.
        neso_df (pd.DataFrame): NESO embedded generation data.

    Returns:
        pd.DataFrame: Combined dataset with fuel types including
        embedded generation.
    """
    # Pivot BMRS so each fuelType becomes a column.
    # Take the mean for each of the 6 5-min averages to get the average for
    # the full 30 minute settlement period.
    pivoted = bmrs_fuelinst_df.pivot_table(
        index="timestamp",
        columns="fuelType",
        values="generation",
        aggfunc="mean",
        fill_value=0,
    ).reset_index()

    merged = pd.merge(pivoted, neso_df, on="timestamp", how="inner")

    return merged


def process_merged(merged):
    """
    Add aggregate energy categories (wind, gas, renewable, fossil fuels,
    low carbon).

    Rounds the data to 2 decimal places.

    Args:
        merged (pd.DataFrame): Combined BMRS + NESO dataset.

    Returns:
        pd.DataFrame: Dataset with additional aggregate columns.
    """
    merged["TOTAL_WIND"] = merged["WIND"] + merged["EMBEDDED_WIND_GENERATION"]
    merged["TOTAL_GAS"] = merged["CCGT"] + merged["OCGT"]

    merged["TOTAL_RENEWABLE"] = (
        merged["TOTAL_WIND"] + merged["EMBEDDED_SOLAR_GENERATION"] + merged["NPSHYD"]
    )

    merged["TOTAL_FOSSIL_FUELS"] = merged["TOTAL_GAS"] + merged["COAL"] + merged["OIL"]
    merged["TOTAL_LOW_CARBON"] = (
        merged["TOTAL_RENEWABLE"] + merged["BIOMASS"] + merged["NUCLEAR"]
    )

    return merged.round(2)


def fetch_pipeline():
    """Fetch, merge, process, and save BMRS + NESO energy data."""
    print("Fetching BMRS data...")
    fuelinst_df = fetch_bmrs_fuelinst(24)

    print("Fetching NESO data...")
    neso_df = fetch_neso_demand(24)

    print("Merging datasets...")
    merged = merge_datasets(fuelinst_df, neso_df)

    print("Processing data...")
    merged = process_merged(merged)

    merged.to_csv("data_pipeline/Data/combined_energy_data.csv", index=False)
    print("Saved combined data with", len(merged), "rows → combined_energy_data.csv")


if __name__ == "__main__":
    fetch_pipeline()
