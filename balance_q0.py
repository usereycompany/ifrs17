import datetime
import timeit

import balance_q1
import pandas as pd
from pandas.tseries.offsets import MonthEnd

"""
portfolio check na zaklade bcm, actuals, TRV a NPR

    TRV prevest do EUR

    NPR nektere cashflow se nasobi 0,003 -- pouze u retra LRH a PRH
        konkretni kody (provize)
        ze zdiskontovanych

"""

ANALYSIS_DATE = balance_q1.ANALYSIS_DATE
CALCULATION_DATE = datetime.datetime.date(pd.Timestamp(ANALYSIS_DATE) + MonthEnd(-3))

NPR = 0.003


def trial():

    df = pd.read_csv("q0/inputs/bcm.csv", delimiter=";", decimal=",")
    result = df.groupby("I17COSTREVENUE").sum()

    # df_life = pd.read_csv("inputs/bcm_life.csv", delimiter=";", decimal=",")
    # df = pd.concat([df, df_life], ignore_index=True, sort=False)

    # df_df_factors = pd.read_csv("inputs/df.csv", delimiter=";", decimal=",")
    # df_fx = pd.read_excel("inputs/fx_rates.xlsx")

    # df_lookup_bcm = pd.read_excel("mapping_tables/lookup_table_bcm.xlsx")
    # df_lookup_portfolio = pd.read_excel("mapping_tables/lookup_local_portfolio.xlsx")
    # cond_x = df["I17REFOBJID"] == "CZR_ACP_PRI_P500_2020_NO_320"
    # cond_x = df["I17REFOBJID"] == "CZR_ACP_LRH_L510_2021_NO_10260/1"

    # df = df[cond_x]

    # result = df.groupby("I17REFOBJID").sum()
    # result.to_excel("contracts.xlsx")
    # df.to_excel("contracts.xlsx")

    result.to_csv("_trial.csv", decimal=",", sep=";")

    return df


def local_portfolio_lookup_table():

    df = pd.read_csv("inputs/BFX_input.csv", delimiter=";", decimal=",")
    df_in_results = pd.read_csv("inputs/results_input.csv", delimiter=";", decimal=",")

    def df_edit(df: pd.DataFrame):

        df = df.rename(
            columns={
                "Assigned Portfolio (Profit Recognition)": "I",
                "Contract/Portfolio": "O",
            }
        )

        cond1 = df["I"].isnull()
        cond2 = df["O"].str.contains("/", na=False)

        df1 = df[cond1 & cond2]
        df1.loc[:, "contract"] = df1["O"].apply(
            lambda x: "_".join((x.split("/")[0]).split("_")[2:3])
            + "_"
            + "_".join((x.split("/")[0]).split("_")[4:7])
        )

        df2 = df[cond1 & ~cond2]
        df2.loc[:, "contract"] = df2["O"].apply(
            lambda x: "_".join(x.split("_")[2:3]) + "_" + "_".join(x.split("_")[4:7])
        )

        df3 = df[~cond1]
        df3.loc[:, "contract"] = df3["I"].apply(lambda x: "_".join(x.split("_")[2:6]))

        df4 = pd.concat([df1, df2, df3], ignore_index=True, sort=False)
        return df4

    df_bfx = df_edit(df)
    df_results = df_edit(df_in_results)

    result_bfx = df_bfx.groupby(["contract", "Local Portfolio"]).sum()
    result_bfx = result_bfx.reset_index()
    result_bfx = result_bfx.drop(
        result_bfx.columns[2 : len(result_bfx.columns)], axis=1
    )

    result_bfx.to_excel("mapping_tables/lookup_local_portfolio.xlsx")

    df_final = pd.merge(
        left=df_results,
        right=result_bfx[["contract", "Local Portfolio"]],
        left_on=["contract"],
        right_on=["contract"],
        how="left",
    )
    df_final.to_excel("results.xlsx")

    return df


def preparation_for_mapping(df: pd.DataFrame):

    df.loc[:, "contract"] = df["I17REFOBJID"]
    df.loc[:, "contract"] = df["contract"].apply(
        lambda x: "_".join(x.split("_")[2:3]) + "_" + "_".join(x.split("_")[4:7])
        if "/" not in x
        else "_".join((x.split("/")[0]).split("_")[2:3])
        + "_"
        + "_".join((x.split("/")[0]).split("_")[4:7])
    )

    df.loc[:, "zivot_nezivot"] = df["I17REFOBJID"]
    df.loc[:, "zivot_nezivot"] = df["zivot_nezivot"].apply(
        lambda x: "zivot" if x.split("_")[2][0] == "L" else "nezivot"
    )

    df.loc[:, "ri_rh"] = df["I17REFOBJID"]
    df.loc[:, "ri_rh"] = df["ri_rh"].apply(lambda x: x.split("_")[2][1:3])

    # df.loc[:, "portfolio"] = df["I17REFOBJID"]
    # df.loc[:, "portfolio"] = df["portfolio"].apply(lambda x: x.split("_")[3])

    return df


def bcm():

    df_life = pd.read_csv("q0/inputs/bcm_life.csv", delimiter=";", decimal=",")
    df = pd.read_csv("q0/inputs/bcm.csv", delimiter=";", decimal=",")
    # df = pd.read_csv("q0/inputs/bcm.csv", delimiter=";", decimal=",", nrows=100_000)
    df = pd.concat([df, df_life], ignore_index=True, sort=False)
    # df = pd.read_excel("_trial_life.xlsx")

    df_df_factors = pd.read_csv("q0/inputs/df.csv", delimiter=";", decimal=",")

    df_fx = pd.read_excel("q0/inputs/fx_rates.xlsx")

    df_lookup_bcm = pd.read_excel("q0/mapping_tables/lookup_table_bcm.xlsx")
    df_lookup_portfolio = pd.read_excel("q0/mapping_tables/lookup_local_portfolio.xlsx")

    df = df.drop(
        columns=[
            "I17PERIOD",
            "I17TIMEST",
            "I17SOLO",
            "I17FUNCSYSTEM",
            "I17EXPCHANGE",
            "I17KEYDATE",
            "I17ITEMID",
        ]
    )

    df.loc[:, "date_adjusted"] = pd.to_datetime(df["I17SETTLEMDATE"], format="%Y%m%d")
    df.loc[:, "date_adjusted"] = df["date_adjusted"].dt.date
    df.loc[:, "date_adjusted"] = df["date_adjusted"] + pd.tseries.offsets.MonthEnd(1)
    df.loc[:, "date_adjusted"] = (
        df["date_adjusted"] + pd.tseries.offsets.MonthEnd(-1)
    ).dt.date
    df.loc[:, "date_adjusted"] = (
        df["date_adjusted"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df = pd.merge(
        left=df,
        right=df_df_factors[["I17DISCDATE", "I17ESTCURR", "I17DISCFACT"]],
        left_on=["date_adjusted", "I17ESTCURR"],
        right_on=["I17DISCDATE", "I17ESTCURR"],
        how="left",
    )

    df = pd.merge(
        left=df,
        right=df_fx[["FROM_CURRENCY", "EXCHANGE_RATE"]],
        left_on="I17ESTCURR",
        right_on="FROM_CURRENCY",
        how="left",
    )

    df = preparation_for_mapping(df)

    df.loc[:, "lrc_lic"] = pd.to_datetime(df["I17INCURREDDATE"], format="%Y%m%d")
    df.loc[:, "lrc_lic"] = df["lrc_lic"].dt.date

    df.loc[:, "lrc_lic"] = df["lrc_lic"].apply(
        lambda x: "LIC" if x <= CALCULATION_DATE else "LRC"
    )

    cond1 = df["date_adjusted"] == int(CALCULATION_DATE.strftime("%Y%m%d"))
    df.loc[cond1, "I17DISCFACT"] = 1

    df = df.drop(columns=["I17DISCDATE", "FROM_CURRENCY"])

    df.loc[:, "nominal_original_CCY"] = df["I17ESTAMOUNT"]
    df.loc[:, "nominal"] = df["nominal_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, "discounted_CF_original_CCY"] = (
        df["I17DISCFACT"] * df["nominal_original_CCY"]
    )
    df.loc[:, "discounted_CF"] = df["discounted_CF_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, "discounted_CF_original_CCY"] = (
        df["discounted_CF_original_CCY"] - df["nominal_original_CCY"]
    )
    df.loc[:, "discount"] = df["discounted_CF"] - df["nominal"]

    df = df.drop(
        columns=[
            "I17INCURREDDATE",
            "I17SETTLEMDATE",
            "I17DISCFACT",
            "EXCHANGE_RATE",
            "I17ESTAMOUNT",
            "date_adjusted",
            "discounted_CF_original_CCY",
            "discounted_CF",
        ]
    )

    df_lookup_portfolio = df_lookup_portfolio.assign(
        contract=df_lookup_portfolio["contract"].astype("str")
    )

    df = df.assign(I17COSTREVENUE=df["I17COSTREVENUE"].astype("str"))

    df_lookup_bcm = df_lookup_bcm.assign(
        I17COSTREVENUE=df_lookup_bcm["I17COSTREVENUE"].astype("str")
    )

    pre_mapping = df.groupby(
        [
            "ri_rh",
            "zivot_nezivot",
            "contract",
            "I17COSTREVENUE",
            "lrc_lic",
        ]
    ).sum()

    pre_mapping = pre_mapping.reset_index()

    df = pd.merge(
        left=pre_mapping,
        right=df_lookup_portfolio,
        left_on="contract",
        right_on="contract",
        how="left",
    )

    df = pd.merge(
        left=df,
        right=df_lookup_bcm,
        left_on=["I17COSTREVENUE", "ri_rh", "lrc_lic"],
        right_on=["I17COSTREVENUE", "ri_rh", "lrc_lic"],
        how="left",
    )

    result = df.groupby(
        [
            # "local_portfolio",
            # "I17ESTCURR",
            # # "I17COSTREVENUE",
            "ri_rh",
            "zivot_nezivot",
            "local_portfolio",
            "account",
        ]
    ).sum()

    result.to_excel("q0/results/result_bcm.xlsx")

    result_codes = df.groupby(
        [
            "ri_rh",
            "zivot_nezivot",
            "I17COSTREVENUE",
            "local_portfolio",
            "lrc_lic",
        ]
    ).sum()

    result_codes.to_excel("q0/results/result_bcm_codes.xlsx")

    return result


def act():
    """
    Actuals se nediskontuji a jsou uvedeny v EUR i puvodni mene
    """
    df = pd.read_csv("q0/inputs/act.csv", delimiter=";", decimal=",")
    df_lookup_act = pd.read_excel("q0/mapping_tables/lookup_table_act.xlsx")
    df_lookup_portfolio = pd.read_excel("q0/mapping_tables/lookup_local_portfolio.xlsx")

    df = df.drop(
        columns=[
            "I17PERIOD",
            "I17TIMEST",
            "I17SOLO",
            "I17FUNCSYSTEM",
            "I17CLASSID",
            "I17CFID",
            "I17CFREV",
            "I17CFDATE1",
            "I17CFDATE2",
        ]
    )
    df = df.rename(
        columns={
            "I17PORTID": "I17REFOBJID",
            "I17CFTYPE": "I17COSTREVENUE",
        }
    )

    df = preparation_for_mapping(df)

    df = df.rename(columns={"I17VALUE": "actuals_CCY", "I17FUNCTVALUE": "actuals"})
    df.loc[:, "lrc_lic"] = "LIC"
    df.loc[:, "actuals_CCY"] = df["actuals_CCY"] * -1
    df.loc[:, "actuals"] = df["actuals"] * -1

    # df.to_excel("result_act.xlsx")

    df_lookup_portfolio = df_lookup_portfolio.assign(
        contract=df_lookup_portfolio["contract"].astype("str")
    )

    pre_mapping = df.groupby(
        [
            "ri_rh",
            "zivot_nezivot",
            "contract",
            "I17COSTREVENUE",
            "lrc_lic",
        ]
    ).sum()

    pre_mapping = pre_mapping.reset_index()

    df = pd.merge(
        left=pre_mapping,
        right=df_lookup_portfolio,
        left_on="contract",
        right_on="contract",
        how="left",
    )

    df = pd.merge(
        left=df,
        right=df_lookup_act,
        left_on=["I17COSTREVENUE", "ri_rh", "lrc_lic"],
        right_on=["I17COSTREVENUE", "ri_rh", "lrc_lic"],
        how="left",
    )

    result = df.groupby(
        [
            "ri_rh",
            "zivot_nezivot",
            "local_portfolio",
            "account",
        ]
    ).sum()

    result.to_excel("q0/results/result_act.xlsx")

    return result


def trv():

    """
    AKA CSM


    v TRV souboru neni ve stringu smlouvy P500 atd. -- je proto nutne zmenit
    """

    df_lookup_portfolio = pd.read_excel("q0/mapping_tables/lookup_local_portfolio.xlsx")

    df_life = pd.read_csv("q0/inputs/trv_life.csv", delimiter=";", decimal=",")
    df = pd.read_csv("q0/inputs/trv.csv", delimiter=";", decimal=",")
    df = pd.concat([df, df_life], ignore_index=True, sort=False)

    df_fx = pd.read_excel("q0/inputs/fx_rates.xlsx")
    """
    mapping je stejny jako u BCM
    """
    df_lookup_trv = pd.read_excel("q0/mapping_tables/lookup_table_bcm.xlsx")

    df = df.drop(
        columns=[
            "I17PERIOD",
            "I17TIMEST",
            "I17SOLO",
            "I17FUNCSYSTEM",
            "I17RESULTKEYDATE",
            "I17CALCMETH",
            "I17FPSLRESCAT",
            "I17OCIBALANCEVFA",
        ]
    )

    df = pd.merge(
        left=df,
        right=df_fx[["FROM_CURRENCY", "EXCHANGE_RATE"]],
        left_on="I17ESTCURR",
        right_on="FROM_CURRENCY",
        how="left",
    )

    df = preparation_for_mapping(df)

    df.loc[:, "contract"] = df["I17REFOBJID"]
    df.loc[:, "contract"] = df["contract"].apply(
        lambda x: "_".join(x.split("_")[2:])
        if "/" not in x
        else "_".join((x.split("/")[0]).split("_")[2:])
    )

    df.loc[:, "trv_original_CCY"] = df["I17ESTAMOUNT"]
    df.loc[:, "trv"] = df["trv_original_CCY"] / df["EXCHANGE_RATE"]

    df = df.drop(
        columns=[
            "EXCHANGE_RATE",
            "I17ESTAMOUNT",
        ]
    )
    df.loc[:, "lrc_lic"] = "LRC"

    df_lookup_portfolio = df_lookup_portfolio.assign(
        contract=df_lookup_portfolio["contract"].astype("str")
    )

    df = df.assign(I17COSTREVENUE=df["I17COSTREVENUE"].astype("str"))

    df_lookup_trv = df_lookup_trv.assign(
        I17COSTREVENUE=df_lookup_trv["I17COSTREVENUE"].astype("str")
    )

    pre_mapping = df.groupby(
        [
            "ri_rh",
            "zivot_nezivot",
            "contract",
            "I17COSTREVENUE",
            "lrc_lic",
        ]
    ).sum()

    pre_mapping = pre_mapping.reset_index()

    df = pd.merge(
        left=pre_mapping,
        right=df_lookup_portfolio,
        left_on="contract",
        right_on="contract",
        how="left",
    )

    df = pd.merge(
        left=df,
        right=df_lookup_trv,
        left_on=["I17COSTREVENUE", "ri_rh", "lrc_lic"],
        right_on=["I17COSTREVENUE", "ri_rh", "lrc_lic"],
        how="left",
    )

    result = df.groupby(
        [
            "ri_rh",
            "zivot_nezivot",
            "local_portfolio",
            "account",
        ]
    ).sum()

    result.to_excel("q0/results/result_trv.xlsx")

    return result


def npr(npr):
    df = pd.read_excel("q0/result_bcm_codes.xlsx")
    df = df.ffill()
    df_lookup_npr = pd.read_excel("q0/mapping_tables/lookup_table_npr.xlsx")

    codes_applicable = [
        "Y103",
        "Y204",
        "Y220",
    ]
    cond = df["I17COSTREVENUE"].isin(codes_applicable)
    df = df[cond]
    df.loc[:, "npr"] = (df["nominal"] + df["discount"]) * npr * -1

    df = pd.merge(
        left=df,
        right=df_lookup_npr,
        left_on=["lrc_lic"],
        right_on=["lrc_lic"],
        how="left",
    )
    df = df.drop(
        columns=[
            "nominal_original_CCY",
            "nominal",
            "discount",
        ]
    )
    result = df.groupby(
        [
            "ri_rh",
            "zivot_nezivot",
            "local_portfolio",
            "account",
        ]
    ).sum()
    result.to_excel("q0/results/result_npr.xlsx")

    return result


def portfolio_balance():
    df_bcm = pd.read_excel("q0/results/result_bcm.xlsx").ffill()
    df_act = pd.read_excel("q0/results/result_act.xlsx").ffill()
    df_trv = pd.read_excel("q0/results/result_trv.xlsx").ffill()
    df_npr = pd.read_excel("q0/results/result_npr.xlsx").ffill()

    total_balance = (
        df_bcm["nominal"].sum()
        + df_bcm["discount"].sum()
        + df_act["actuals"].sum()
        + df_trv["trv"].sum()
        + df_npr["npr"].sum()
    )
    bcm = df_bcm["nominal"].sum()
    discount = df_bcm["discount"].sum()
    act = df_act["actuals"].sum()
    trv = df_trv["trv"].sum()
    npr = df_npr["npr"].sum()

    # print(f"BCM {round(bcm)}")
    # print(f"DIS {round(discount)}")
    # print(f"ACT {round(act)}")
    # print(f"TRV {round(trv)}")
    # print(f"NPR {round(npr)}")
    print(f"Total is {round(total_balance)}")

    df = pd.merge(
        left=df_bcm,
        right=df_act,
        left_on=["ri_rh", "zivot_nezivot", "local_portfolio", "account"],
        right_on=["ri_rh", "zivot_nezivot", "local_portfolio", "account"],
        how="outer",
    )

    df = pd.merge(
        left=df,
        right=df_trv,
        left_on=["ri_rh", "zivot_nezivot", "local_portfolio", "account"],
        right_on=["ri_rh", "zivot_nezivot", "local_portfolio", "account"],
        how="outer",
    )

    df = pd.merge(
        left=df,
        right=df_npr,
        left_on=["ri_rh", "zivot_nezivot", "local_portfolio", "account"],
        right_on=["ri_rh", "zivot_nezivot", "local_portfolio", "account"],
        how="outer",
    )

    total_balance = (
        df["nominal"].sum()
        + df["discount"].sum()
        + df["actuals"].sum()
        + df["trv"].sum()
        + df["npr"].sum()
    )
    print(f"Total is {round(total_balance)}")

    columns_to_sum = [
        "nominal",
        "discount",
        "actuals",
        "trv",
        "npr",
    ]

    df.loc[:, "total_balance"] = df[columns_to_sum].sum(axis=1)

    df = df.drop(
        columns=[
            "nominal_original_CCY",
            "nominal",
            "discount",
            "actuals_CCY",
            "actuals",
            "trv_original_CCY",
            "trv",
            "npr",
        ]
    )

    df_flag = df.groupby(
        [
            "ri_rh",
            "local_portfolio",
        ]
    ).sum()

    df_flag = df_flag.reset_index()
    df_flag = df_flag.ffill()

    cond1 = df_flag["ri_rh"] == "RH"
    cond2 = df_flag["total_balance"] < 0
    cond3 = df_flag["ri_rh"] == "RI"
    cond4 = df_flag["total_balance"] > 0

    df_flag.loc[:, "mapping_change_flag"] = False
    df_flag.loc[(cond1 & cond2) | (cond3 & cond4), "mapping_change_flag"] = True

    df1 = pd.merge(
        left=df,
        right=df_flag[["local_portfolio", "mapping_change_flag"]],
        left_on=["local_portfolio"],
        right_on=["local_portfolio"],
        how="left",
    )

    cond5 = df1["mapping_change_flag"] == True

    df1_wo_change = df1[~cond5]
    df1_w_change = df1[cond5]

    df_lookup_portfolio = pd.read_excel("q0/mapping_tables/lookup_table_portfolio.xlsx")
    df1_w_change = pd.merge(
        left=df1_w_change,
        right=df_lookup_portfolio,
        left_on=["account"],
        right_on=["account"],
        how="left",
    )
    df1_w_change = df1_w_change.drop(
        columns=[
            "account",
            "mapping_change_flag",
        ]
    )
    df1_w_change = df1_w_change.rename(columns={"counter_account": "account"})

    columns_to_show = ["account", "total_balance"]

    df1_w_change = df1_w_change[columns_to_show]
    df1_wo_change = df1_wo_change[columns_to_show]

    df_final = pd.concat([df1_wo_change, df1_w_change], ignore_index=True, sort=False)

    # df_final.to_excel("2result_fs.xlsx")
    result = df_final.groupby(
        [
            "account",
        ]
    ).sum()

    """
    
    doplnit protizapisy -- zatim vse na I914000010
    
    """
    result.to_excel("q0/results/result_fs.xlsx")

    return result


def main():
    """
    filtrovani jedne smlouvy pro input data

    """
    # trial()
    # local_portfolio_lookup_table()

    # bcm()
    # act()
    # trv()
    # npr(NPR)
    # portfolio_balance()


if __name__ == "__main__":
    started = (
        datetime.datetime.utcnow()
        .replace(tzinfo=datetime.timezone.utc)
        .astimezone(tz=None)
    ).strftime("%H:%M:%S")
    starttime = timeit.default_timer()
    print(started)
    main()
    print("Time elapsed:", timeit.default_timer() - starttime)
