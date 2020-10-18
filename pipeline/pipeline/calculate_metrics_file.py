import numpy as np
import pandas as pd
from numpy import random
from scipy.interpolate import CubicSpline


def imputeCols(col):
    col = pd.Series(np.where(col < 0, np.NaN, col))
    preNaNs = (
        col.isnull()
        .astype(int)
        .groupby(col.notnull().astype(int).cumsum())
        .cumsum()
        .shift(1)
        + 1
    )
    avgs = np.round(col / preNaNs)
    avgs = avgs.bfill()
    avgs = np.where(np.logical_or(np.isnan(col), avgs < col), avgs, col)
    avgs = pd.Series(avgs)
    avgs.fillna(np.round(avgs.mean()), inplace=True)
    return avgs.to_numpy()


def generate_hospitalizations(df, hospitalizations):
    try:
        df_bck = pd.read_csv(hospitalizations, index_col=["date"])
    except Exception as e:
        print("Unable to find hospitalizatons file. Re-generating data")
        df["percentages"] = random.uniform(0.12, 0.16, size=df.shape[0])
        df_bck = pd.DataFrame(df.pop("percentages"), index=df.index)
        df_bck.to_csv(hospitalizations)
    df = df.join(df_bck)
    if sum(np.isnan(df["percentages"])) > 0:
        imp = pd.Series(
            random.uniform(0.12, 0.16, size=sum(np.isnan(df["percentages"])))
        )
        df.loc[np.isnan(df["percentages"]), ["percentages"]] = imp[0]
        df_bck = pd.DataFrame(df["percentages"], index=df.index)
        df_bck.to_csv(hospitalizations)
    return df.pop("percentages") * df["delta.confirmed"]


def cubic_spline(col):
    cs = CubicSpline(range(len(col)), col)
    return cs(range(len(col)))


def calculate_metrics(
    df,
    start_date="2020-04-20",
    hospitalizations_csv="output/percentages_for_hospitalizations.csv",
    output_city_metrics_csv="output/city_metrics.csv",
    header=True,
):
    # add "other" columns
    if not "delta.other" in df.columns:
        df["delta.other"] = 0
    if not "total.other" in df.columns:
        df["total.other"] = 0

    df.loc[df["delta.other"] < 0, ["delta.other"]] = np.nan

    # we start all the data from this date as a baseline date
    drop_rows = df[df.index < start_date]
    # print(drop_rows.index[0:5])
    df.drop(drop_rows.index, axis=0, inplace=True)

    # impute data
    df["delta.tested"] = imputeCols(df["delta.tested"])
    df["delta.confirmed"] = imputeCols(df["delta.confirmed"])
    df["delta.deceased"] = imputeCols(df["delta.deceased"])
    df["delta.recovered"] = imputeCols(df["delta.recovered"])
    df["delta.other"] = imputeCols(df["delta.other"])

    # generate new "total" columns
    df["total.confirmed"] = df["delta.confirmed"].cumsum()
    df["total.tested"] = df["delta.tested"].cumsum()
    df["total.recovered"] = df["delta.recovered"].cumsum()
    df["total.deceased"] = df["delta.deceased"].cumsum()
    df["total.other"] = df["delta.other"].cumsum()

    # generate Levitt Metric
    df["total.deceased.shift"] = df["total.deceased"].shift(1)
    df["levitt.Metric"] = np.log(df["total.deceased"] / df.pop("total.deceased.shift"))

    # 21-Day MA of daily tests
    df["MA.21.daily.tests"] = df["delta.tested"].rolling(window=21).mean()

    # TPR% per day
    df["delta.positivity"] = (df["delta.confirmed"] / df["delta.tested"]) * 100.0

    # 21-Day MA of TPR%
    df["MA.21.delta.positivity"] = df["delta.positivity"].rolling(window=21).mean()

    # daily percent case growth
    df["delta.percent.case.growth"] = df["delta.confirmed"].pct_change()

    # Impute hospitalization data
    df["delta.hospitalized"] = generate_hospitalizations(df, hospitalizations_csv)

    # total hospitalizations
    df["total.hospitalized"] = df["delta.hospitalized"].cumsum()

    # active cases by day
    df["delta.active"] = (
        df["total.confirmed"]
        - df["total.deceased"]
        - df["total.recovered"]
        - df["delta.other"]
    )

    # cubic splines
    df["spline.active"] = cubic_spline(df["delta.active"])
    df["spline.deceased"] = cubic_spline(df["delta.deceased"])
    df["spline.hospitalized"] = cubic_spline(df["delta.hospitalized"])
    df["spline.recovered"] = cubic_spline(df["delta.recovered"])

    df.to_csv(output_city_metrics_csv, mode="w" if header else "a", header=header)


# df = pd.read_csv("output/city_stats.csv", index_col=["date"])
# df.index = pd.to_datetime(df.index)
##df = df[df.district == "Mumbai"]
# calculate_metrics(df=df)
