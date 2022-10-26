import datetime
import timeit

import pandas as pd

"""
portfolio check na zaklade bcm, actuals, csm a NPR

    csm prevest do EUR

    NPR nektere cashflow se nasobi 0,003 -- pouze u retra LRH a PRH
        konkretni kody (provize)
        ze zdiskontovanych

"""

ANALYSIS_DATE = "31.03.2022"
NPR = 0.003


def trial():

    df = pd.read_csv("q1/inputs/bcm.csv", delimiter=";", decimal=",")
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
    result = df.groupby(
        [
            "I17COSTREVENUE",
        ]
    ).sum()
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
        lambda x: "_".join(x.split("_")[-2:])
        if "/" not in x
        else "_".join((x.split("/")[0]).split("_")[-2:])
    )
    # df.loc[:, "contract"] = df["contract"].apply(
    #     lambda x: "_".join(x.split("_")[2:3]) + "_" + "_".join(x.split("_")[4:7])
    #     if "/" not in x
    #     else "_".join((x.split("/")[0]).split("_")[2:3])
    #     + "_"
    #     + "_".join((x.split("/")[0]).split("_")[4:7])
    # )

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

    # df_life = pd.read_csv("q1/inputs/bcm_life.csv", delimiter=";", decimal=",")
    df = pd.read_csv("q1/inputs/bcm.csv", delimiter=";", decimal=",")
    # df = pd.read_csv("q1/inputs/bcm.csv", delimiter=";", decimal=",", nrows=100_000)
    # df = pd.concat([df, df_life], ignore_index=True, sort=False)
    # df = pd.read_excel("_trial_life.xlsx")

    df_df_factors_current = pd.read_csv(
        "q1/inputs/df_current.csv", delimiter=";", decimal=","
    )
    df_df_factors_locked_in_shifted = pd.read_csv(
        "q1/inputs/df_locked_in.csv", delimiter=";", decimal=","
    )

    df_df_factors_current = df_df_factors_current.rename(
        columns={
            "I17DISCFACT": "I17DISCFACT_current",
        }
    )

    df_fx = pd.read_excel("q1/inputs/fx_rates.xlsx")
    df_fx_balance = df_fx[df_fx["type"] == "M"]
    df_fx_pl = df_fx[df_fx["type"] == "AVG"]

    df_lookup_bcm = pd.read_excel("q1/mapping_tables/lookup_table_bcm.xlsx")
    df_old_contracts = pd.read_excel("q1/mapping_tables/old_contracts.xlsx")
    df_lookup_portfolio = pd.read_excel("q1/mapping_tables/lookup_local_portfolio.xlsx")

    df = df[df["I17EXPCHANGE"] == 60]

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
        right=df_fx_balance[
            [
                "FROM_CURRENCY",
                "EXCHANGE_RATE",
            ]
        ],
        left_on="I17ESTCURR",
        right_on="FROM_CURRENCY",
        how="left",
    )

    df.loc[df["I17ESTCURR"] == "EUR", "EXCHANGE_RATE"] = 1

    df = preparation_for_mapping(df)

    # V BUDOUCNU:
    # merge s local_portfolio_lookup_table a pak ~ = Q z calculation_date
    # pro old contracts udelat dict s nazvy DF filu a matchovat to na flag z local_portfolio_lookup_table

    cond_period = df["contract"].isin(df_old_contracts["contract"])
    df_old_contracts = df[cond_period]
    df_new_contracts = df[~cond_period]

    df_old_contracts.loc[:, "origin"] = "old"

    df_old_contracts = pd.merge(
        left=df_old_contracts,
        right=df_df_factors_locked_in_shifted[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                "I17DISCFACT_locked_in",
            ]
        ],
        left_on=["date_adjusted", "I17ESTCURR"],
        right_on=["I17DISCDATE", "I17ESTCURR"],
        how="left",
    )
    df_old_contracts = df_old_contracts.drop(
        columns=[
            "I17DISCDATE",
        ]
    )

    df_df_factors_current.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_factors_current["I17DISCDATE"], format="%d.%m.%Y"
    )

    df_df_factors_current.loc[:, "I17DISCDATE"] = (
        df_df_factors_current["I17DISCDATE"]
        .apply(lambda x: x.strftime("%Y%m%d"))
        .astype(int)
    )

    df_old_contracts = pd.merge(
        left=df_old_contracts,
        right=df_df_factors_current[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                "I17DISCFACT_current",
            ]
        ],
        left_on=["date_adjusted", "I17ESTCURR"],
        right_on=["I17DISCDATE", "I17ESTCURR"],
        how="left",
    )

    df_old_contracts = df_old_contracts.drop(
        columns=[
            "I17DISCDATE",
        ]
    )

    df_new_contracts.loc[:, "origin"] = "new"

    df_new_contracts = pd.merge(
        left=df_new_contracts,
        right=df_df_factors_current[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                "I17DISCFACT_current",
            ]
        ],
        left_on=["date_adjusted", "I17ESTCURR"],
        right_on=["I17DISCDATE", "I17ESTCURR"],
        how="left",
    )
    df_new_contracts.loc[:, "I17DISCFACT_locked_in"] = df_new_contracts[
        "I17DISCFACT_current"
    ]
    df_new_contracts = df_new_contracts.drop(
        columns=[
            "I17DISCDATE",
        ]
    )

    df = pd.concat([df_old_contracts, df_new_contracts], ignore_index=True, sort=False)

    # ===================== LRC/LIC

    df.loc[:, "lrc_lic"] = pd.to_datetime(df["I17INCURREDDATE"], format="%Y%m%d")
    df.loc[:, "lrc_lic"] = df["lrc_lic"].dt.date

    calculation_date = datetime.datetime.date(pd.Timestamp(ANALYSIS_DATE))

    df.loc[:, "lrc_lic"] = df["lrc_lic"].apply(
        lambda x: "LIC" if x <= calculation_date else "LRC"
    )
    # ===================== LRC/LIC

    cond1 = df["date_adjusted"] == int(calculation_date.strftime("%Y%m%d"))
    df.loc[cond1, ["I17DISCFACT_locked_in", "I17DISCFACT_current"]] = 1

    df.loc[:, "nominal_original_CCY"] = df["I17ESTAMOUNT"]
    df.loc[:, "nominal"] = df["nominal_original_CCY"] / df["EXCHANGE_RATE"]

    df.loc[:, "dcf_locked_in_original_CCY"] = (
        df["I17DISCFACT_locked_in"] * df["nominal_original_CCY"]
    )
    df.loc[:, "dcf_current_original_CCY"] = (
        df["I17DISCFACT_current"] * df["nominal_original_CCY"]
    )

    df.loc[:, "dcf_locked_in"] = df["dcf_locked_in_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, "dcf_current"] = df["dcf_current_original_CCY"] / df["EXCHANGE_RATE"]

    df.loc[:, "discount_locked_in"] = df["dcf_locked_in"] - df["nominal"]
    df.loc[:, "discount_current"] = df["dcf_current"] - df["nominal"]

    df = df.drop(
        columns=[
            "I17INCURREDDATE",
            "I17SETTLEMDATE",
            "I17DISCFACT_locked_in",
            "I17DISCFACT_current",
            "FROM_CURRENCY",
            "EXCHANGE_RATE",
            "I17ESTAMOUNT",
            "date_adjusted",
            "dcf_locked_in_original_CCY",
            "dcf_current_original_CCY",
            "dcf_locked_in",
            "dcf_current",
            # "origin",
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
            "origin",
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
            "origin",
            "ri_rh",
            "zivot_nezivot",
            "I17COSTREVENUE",
            "account",
        ]
    ).sum()

    result.to_excel("q1/results/break/result_bcm.xlsx")

    result_codes = df.groupby(
        [
            "origin",
            "ri_rh",
            "zivot_nezivot",
            "I17COSTREVENUE",
            "local_portfolio",
            "lrc_lic",
        ]
    ).sum()

    result_codes.to_excel("q1/results/break/result_bcm_codes.xlsx")

    return result


def act():
    """
    Actuals se nediskontuji a jsou uvedeny v EUR i puvodni mene
    """
    df = pd.read_csv("q1/inputs/act.csv", delimiter=";", decimal=",")
    df_lookup_act = pd.read_excel("q1/mapping_tables/lookup_table_act.xlsx")
    df_lookup_portfolio = pd.read_excel("q1/mapping_tables/lookup_local_portfolio.xlsx")

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

    """
    doplneni new/old
    """
    df_old_contracts = pd.read_excel("q1/mapping_tables/old_contracts.xlsx")

    cond_period = df["contract"].isin(df_old_contracts["contract"])
    df_old_contracts = df[cond_period]
    df_new_contracts = df[~cond_period]

    df_old_contracts.loc[:, "origin"] = "old"
    df_new_contracts.loc[:, "origin"] = "new"
    df = pd.concat([df_old_contracts, df_new_contracts], ignore_index=True, sort=False)

    """
    end
    """

    df = df.rename(columns={"I17VALUE": "actuals_CCY", "I17FUNCTVALUE": "actuals"})

    df.loc[:, "actuals_CCY"] = df["actuals_CCY"] * -1
    df.loc[:, "actuals"] = df["actuals"] * -1

    # df.to_excel("result_act.xlsx")

    df_lookup_portfolio = df_lookup_portfolio.assign(
        contract=df_lookup_portfolio["contract"].astype("str")
    )

    pre_mapping = df.groupby(
        [
            "origin",
            "ri_rh",
            "zivot_nezivot",
            "contract",
            "I17COSTREVENUE",
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
        left_on=[
            "I17COSTREVENUE",
            "ri_rh",
        ],
        right_on=[
            "I17COSTREVENUE",
            "ri_rh",
        ],
        how="left",
    )

    result = df.groupby(
        [
            "origin",
            "ri_rh",
            "zivot_nezivot",
            "I17COSTREVENUE",
            "account",
        ]
    ).sum()

    result.to_excel("q1/results/break/result_act.xlsx")

    return result


def csm():

    df_lookup_portfolio = pd.read_excel("q1/mapping_tables/lookup_local_portfolio.xlsx")

    df_life = pd.read_csv("q1/inputs/csm_life.csv", delimiter=";", decimal=",")
    df = pd.read_csv("q1/inputs/csm.csv", delimiter=";", decimal=",")
    df = pd.concat([df, df_life], ignore_index=True, sort=False)

    df_fx = pd.read_excel("q1/inputs/fx_rates.xlsx")
    """
    mapping je stejny jako u BCM
    """
    df_lookup_csm = pd.read_excel("q1/mapping_tables/lookup_table_bcm.xlsx")

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

    df.loc[:, "csm_original_CCY"] = df["I17ESTAMOUNT"]
    df.loc[:, "csm"] = df["csm_original_CCY"] / df["EXCHANGE_RATE"]

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

    df_lookup_csm = df_lookup_csm.assign(
        I17COSTREVENUE=df_lookup_csm["I17COSTREVENUE"].astype("str")
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
        right=df_lookup_csm,
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

    result.to_excel("q1/results/break/result_csm.xlsx")

    return result


def npr(npr):
    df = pd.read_excel("q1/results/break/result_bcm_codes.xlsx")
    df = df.ffill()
    df_lookup_npr = pd.read_excel("q1/mapping_tables/lookup_table_npr.xlsx")

    codes_applicable = [
        "Y103",
        "Y204",
        "Y220",
    ]
    cond = df["I17COSTREVENUE"].isin(codes_applicable)
    df = df[cond]
    df.loc[:, "npr"] = (df["nominal"] + df["discount_current"]) * npr * -1

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
            "discount_current",
            "discount_locked_in",
        ]
    )
    result = df.groupby(
        [
            "origin",
            "ri_rh",
            "zivot_nezivot",
            "I17COSTREVENUE",
            "account",
        ]
    ).sum()
    result.to_excel("q1/results/break/result_npr.xlsx")

    return result


def portfolio_balance():
    df_bcm = pd.read_excel("q1/results/break/result_bcm.xlsx").ffill()
    df_act = pd.read_excel("q1/results/break/result_act.xlsx").ffill()
    # df_csm = pd.read_excel("q1/results/break/result_csm.xlsx").ffill()
    df_npr = pd.read_excel("q1/results/break/result_npr.xlsx").ffill()

    # total_balance = (
    #     df_bcm["nominal"].sum()
    #     + df_bcm["discount"].sum()
    #     + df_act["actuals"].sum()
    #     # + df_csm["csm"].sum()
    #     + df_npr["npr"].sum()
    # )
    # bcm = df_bcm["nominal"].sum()
    # discount = df_bcm["discount"].sum()
    # act = df_act["actuals"].sum()
    # # csm = df_csm["csm"].sum()
    # npr = df_npr["npr"].sum()

    # print(f"BCM {round(bcm)}")
    # print(f"DIS {round(discount)}")
    # print(f"ACT {round(act)}")
    # print(f"csm {round(csm)}")
    # print(f"NPR {round(npr)}")
    # print(f"Total is {round(total_balance)}")

    df = pd.merge(
        left=df_bcm,
        right=df_act,
        left_on=["origin", "ri_rh", "zivot_nezivot", "I17COSTREVENUE", "account"],
        right_on=["origin", "ri_rh", "zivot_nezivot", "I17COSTREVENUE", "account"],
        how="outer",
    )

    # df = pd.merge(
    #     left=df,
    #     right=df_csm,
    #     left_on=["ri_rh", "zivot_nezivot", "local_portfolio", "account"],
    #     right_on=["ri_rh", "zivot_nezivot", "local_portfolio", "account"],
    #     how="outer",
    # )

    df = pd.merge(
        left=df,
        right=df_npr,
        left_on=["origin", "ri_rh", "zivot_nezivot", "I17COSTREVENUE", "account"],
        right_on=["origin", "ri_rh", "zivot_nezivot", "I17COSTREVENUE", "account"],
        how="outer",
    )

    # total_balance = (
    #     df["nominal"].sum()
    #     + df["discount"].sum()
    #     + df["actuals"].sum()
    #     # + df["csm"].sum()
    #     + df["npr"].sum()
    # )
    # print(f"Total is {round(total_balance)}")

    columns_to_sum = [
        "nominal",
        "discount_current",
        "actuals",
        # "csm",
        "npr",
    ]

    df.loc[:, "total_balance"] = df[columns_to_sum].sum(axis=1)

    df = df.drop(
        columns=[
            # "nominal_original_CCY",
            # "nominal",
            # "discount_current",
            "discount_locked_in",
            "actuals_CCY",
            # "actuals",
            # "csm_original_CCY",
            # "csm",
            # "npr",
        ]
    )

    # FLAGGING

    # df_flag = df.groupby(
    #     [
    #         "ri_rh",
    #         "local_portfolio",
    #     ]
    # ).sum()

    # df_flag = df_flag.reset_index()
    # df_flag = df_flag.ffill()

    # cond1 = df_flag["ri_rh"] == "RH"
    # cond2 = df_flag["total_balance"] < 0
    # cond3 = df_flag["ri_rh"] == "RI"
    # cond4 = df_flag["total_balance"] > 0

    # df_flag.loc[:, "mapping_change_flag"] = False
    # df_flag.loc[(cond1 & cond2) | (cond3 & cond4), "mapping_change_flag"] = True

    # df1 = pd.merge(
    #     left=df,
    #     right=df_flag[["local_portfolio", "mapping_change_flag"]],
    #     left_on=["local_portfolio"],
    #     right_on=["local_portfolio"],
    #     how="left",
    # )

    # cond5 = df1["mapping_change_flag"] == True

    # df1_wo_change = df1[~cond5]
    # df1_w_change = df1[cond5]

    # df_lookup_portfolio = pd.read_excel("q1/mapping_tables/lookup_table_portfolio.xlsx")
    # df1_w_change = pd.merge(
    #     left=df1_w_change,
    #     right=df_lookup_portfolio,
    #     left_on=["account"],
    #     right_on=["account"],
    #     how="left",
    # )
    # df1_w_change = df1_w_change.drop(
    #     columns=[
    #         "account",
    #         "mapping_change_flag",
    #     ]
    # )
    # df1_w_change = df1_w_change.rename(columns={"counter_account": "account"})

    # columns_to_show = ["account", "total_balance"]

    # df1_w_change = df1_w_change[columns_to_show]
    # df1_wo_change = df1_wo_change[columns_to_show]

    # df_final = pd.concat([df1_wo_change, df1_w_change], ignore_index=True, sort=False)

    # df_final.to_excel("2result_fs.xlsx")
    # result = df_final.groupby(
    #     [
    #         "account",
    #     ]
    # ).sum()

    result = df.groupby(
        [
            "origin",
            "account",
            "I17COSTREVENUE",
        ]
    ).sum()

    """
    
    doplnit protizapisy -- zatim vse na I914000010
    
    """
    result.to_excel("q1/results/break/result_fs.xlsx")

    return result


def main():
    """
    filtrovani jedne smlouvy pro input data

    """
    # trial()
    # local_portfolio_lookup_table()

    bcm()
    act()
    # csm()
    npr(NPR)
    portfolio_balance()


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
