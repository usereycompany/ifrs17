import datetime
import time

import pandas as pd

"""
1) napocet PL z BCM

vstupy:
1) upravena BCM (IR a SM)

vystupy:
1) group po GL, contract

Pozn:
1)      BCM IR - hodnoty jdou opacnym znamenkem, INCURRED/SETTLEMENT date pouzito dle COSTREVENUE kodu, pak vsechny Q1 balance
2)      BCM SM - BCM IR - LRC (po 31.3.)
3)      BCM SM - BCM IR - LIC vsechny (tj. LIC balance SM porovnana s IR)

"""

ANALYSIS_DATE = "31.03.2022"
calculation_date = datetime.datetime.date(pd.Timestamp(ANALYSIS_DATE))


def discount(
    df: pd.DataFrame,
    df_df_1: pd.DataFrame,
    df_df_2: pd.DataFrame,
    name1: str,
    name2: str,
):

    df = pd.merge(
        left=df,
        right=df_df_1[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                f"I17DISCFACT_{name1}",
            ]
        ],
        left_on=["date_adjusted", "I17ESTCURR"],
        right_on=["I17DISCDATE", "I17ESTCURR"],
        how="left",
    )

    df = pd.merge(
        left=df,
        right=df_df_2[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                f"I17DISCFACT_{name2}",
            ]
        ],
        left_on=["date_adjusted", "I17ESTCURR"],
        right_on=["I17DISCDATE", "I17ESTCURR"],
        how="left",
    )

    cond1 = df["date_adjusted"] == int(calculation_date.strftime("%Y%m%d"))
    df.loc[
        cond1,
        [
            f"I17DISCFACT_{name1}",
            f"I17DISCFACT_{name2}",
        ],
    ] = 1

    df.loc[:, "nominal_original_CCY"] = df["I17ESTAMOUNT"]
    df.loc[:, "nominal"] = df["nominal_original_CCY"] / df["EXCHANGE_RATE"]

    rounding_exception_CCY = [
        "HUF",
        "ISK",
        "JPY",
        "KRW",
    ]
    cond_exception = df["I17ESTCURR"].isin(rounding_exception_CCY)
    df.loc[cond_exception, "nominal"] = df["nominal"].apply(lambda x: round(x))

    df.loc[:, f"dcf_{name1}_original_CCY"] = (
        df[f"I17DISCFACT_{name1}"] * df["nominal_original_CCY"]
    )
    df.loc[:, f"dcf_{name2}_original_CCY"] = (
        df[f"I17DISCFACT_{name2}"] * df["nominal_original_CCY"]
    )

    df.loc[:, f"dcf_{name1}"] = df[f"dcf_{name1}_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, f"dcf_{name2}"] = df[f"dcf_{name2}_original_CCY"] / df["EXCHANGE_RATE"]

    df = df.fillna(0)
    df.loc[cond_exception, f"dcf_{name1}"] = df[f"dcf_{name1}"].apply(
        lambda x: round(x)
    )
    df.loc[cond_exception, f"dcf_{name2}"] = df[f"dcf_{name2}"].apply(
        lambda x: round(x)
    )

    df.loc[:, f"discount_{name1}"] = df[f"dcf_{name1}"] - df["nominal"]
    df.loc[:, f"discount_{name2}"] = df[f"dcf_{name2}"] - df["nominal"]

    df.loc[:, f"discount_{name1}_original_CCY"] = (
        df[f"dcf_{name1}_original_CCY"] - df["nominal_original_CCY"]
    )
    df.loc[:, f"discount_{name2}_original_CCY"] = (
        df[f"dcf_{name2}_original_CCY"] - df["nominal_original_CCY"]
    )

    return df


def preparation_for_mapping_ir(df: pd.DataFrame):

    df.loc[:, "I17REFOBJID"] = df["contract"]
    df.loc[:, "contract"] = df["contract"].apply(
        lambda x: "_".join((x.split("_")[2], "_".join(x.split("_")[-3:])))
        if "/" not in x
        else "_".join(
            (
                (x.split("/")[0]).split("_")[2],
                "_".join((x.split("/")[0]).split("_")[-3:]),
            )
        )
    )

    # df.loc[:, "zivot_nezivot"] = df["I17REFOBJID"]
    # df.loc[:, "zivot_nezivot"] = df["zivot_nezivot"].apply(
    #     lambda x: "zivot" if x.split("_")[2][0] == "L" else "nezivot"
    # )

    incurred_list = [
        "Y204",
        "Z204",
        "Y220",
        "Z220",
        "Y600",
        "Z600",
        "Z400",
        "Y400",
        "Z303",
        "Y303",
    ]
    settlement_list = [
        "Y100",
        "Z100",
        "Y103",
        "Z103",
    ]

    df.loc[:, "ri_rh"] = df["I17REFOBJID"]
    df.loc[:, "ri_rh"] = df["ri_rh"].apply(lambda x: x.split("_")[2][1:3])

    lrc_lic_incurred = df["I17COSTREVENUE"].isin(incurred_list)
    lrc_lic_settlement = df["I17COSTREVENUE"].isin(settlement_list)

    df.loc[lrc_lic_incurred, "lrc_lic"] = df["incurred"]
    df.loc[lrc_lic_settlement, "lrc_lic"] = df["settlement"]

    df.loc[lrc_lic_incurred, "date_select"] = df["incurred"]
    df.loc[lrc_lic_settlement, "date_select"] = df["settlement"]

    cond_lic = df["lrc_lic"] == "till 31.12.2021"

    df_lic = df[cond_lic]
    df_lic.loc[:, "lrc_lic"] = "LIC"

    df_lrc = df[~cond_lic]
    df_lrc.loc[:, "lrc_lic"] = "LRC"

    df = pd.concat([df_lrc, df_lic], ignore_index=True, sort=False)

    return df


def preparation_for_mapping_sm(df: pd.DataFrame):

    df.loc[:, "I17REFOBJID"] = df["contract"]
    df.loc[:, "contract"] = df["contract"].apply(
        lambda x: "_".join((x.split("_")[2], "_".join(x.split("_")[-3:])))
        if "/" not in x
        else "_".join(
            (
                (x.split("/")[0]).split("_")[2],
                "_".join((x.split("/")[0]).split("_")[-3:]),
            )
        )
    )

    # df.loc[:, "zivot_nezivot"] = df["I17REFOBJID"]
    # df.loc[:, "zivot_nezivot"] = df["zivot_nezivot"].apply(
    #     lambda x: "zivot" if x.split("_")[2][0] == "L" else "nezivot"
    # )

    df.loc[:, "ri_rh"] = df["I17REFOBJID"]
    df.loc[:, "ri_rh"] = df["ri_rh"].apply(lambda x: x.split("_")[2][1:3])

    incurred_list = [
        "Y204",
        "Z204",
        "Y220",
        "Z220",
        "Y600",
        "Z600",
        "Z400",
        "Y400",
        "Z303",
        "Y303",
    ]
    settlement_list = [
        "Y100",
        "Z100",
        "Y103",
        "Z103",
    ]

    lrc_lic_incurred = df["I17COSTREVENUE"].isin(incurred_list)
    lrc_lic_settlement = df["I17COSTREVENUE"].isin(settlement_list)

    df.loc[lrc_lic_incurred, "lrc_lic"] = df["incurred"]
    df.loc[lrc_lic_settlement, "lrc_lic"] = df["settlement"]

    df.loc[lrc_lic_incurred, "date_select"] = df["incurred"]
    df.loc[lrc_lic_settlement, "date_select"] = df["settlement"]

    cond1 = df["lrc_lic"] == "till 31.12.2021"
    cond2 = df["lrc_lic"] == "Q1"

    df_lic = df[cond1 | cond2]
    df_lic.loc[:, "lrc_lic"] = "LIC"

    df_lrc = df[~(cond1 | cond2)]
    df_lrc.loc[:, "lrc_lic"] = "LRC"

    df = pd.concat([df_lrc, df_lic], ignore_index=True, sort=False)

    return df


def rounding_CCY(df: pd.DataFrame, *args):

    rounding_exception_CCY = [
        "HUF",
        "ISK",
        "JPY",
        "KRW",
    ]
    cond_exception = df["I17ESTCURR"].isin(rounding_exception_CCY)

    for name in args:
        df.loc[cond_exception, name] = df[name].apply(lambda x: round(x))

    return df


def bcm():

    df_ir = pd.read_csv(
        "q1/results/ir/result_bcm_ir_v3.csv", delimiter=";", decimal=","
    )
    df_sm = pd.read_csv(
        "q1/results/ir/result_bcm_sm_v3.csv", delimiter=";", decimal=","
    )
    # df_ir = pd.read_csv(
    #     "q1/results/ir/result_bcm_ir_v2.csv", delimiter=";", decimal=",", nrows=100_000
    # )
    # df_sm = pd.read_csv(
    #     "q1/results/ir/result_bcm_sm_v2.csv", delimiter=";", decimal=",", nrows=100_000
    # )

    df_lookup_bcm_IR = pd.read_excel("q1/mapping_tables/lookup_table_bcm_IR.xlsx")
    df_lookup_bcm_IR_SM_lrc = pd.read_excel(
        "q1/mapping_tables/lookup_table_bcm_IR_SM_lrc.xlsx"
    )
    df_lookup_bcm_IR_SM_lic_past = pd.read_excel(
        "q1/mapping_tables/lookup_table_bcm_IR_SM_lic.xlsx"
    )
    df_lookup_bcm_IR_SM_lic_current = pd.read_excel(
        "q1/mapping_tables/lookup_table_bcm_IR_SM_lic_q1.xlsx"
    )
    # df_lookup_portfolio = pd.read_excel("q1/mapping_tables/lookup_local_portfolio.xlsx")

    name1 = "tyc"
    name2 = "ycu_31_12"
    name3 = "tyc_shift"
    name4 = "ycu_31_12_shift"

    df_ir = preparation_for_mapping_ir(df_ir)
    df_sm = preparation_for_mapping_sm(df_sm)

    df_fx = pd.read_excel("q1/inputs/fx_rates.xlsx")
    df_fx_pl = df_fx[df_fx["type"] == "AVG"]
    df_fx_pl = df_fx_pl[
        [
            "FROM_CURRENCY",
            "EXCHANGE_RATE",
        ]
    ]
    df_fx_pl.loc[len(df_fx_pl.index)] = ["EUR", 1]

    """
    1)     BCM IR 
    """

    cond_ir1 = df_ir["date_select"] == "till 31.12.2021"
    cond_ir2 = df_ir["date_select"] == "Q1"

    df_ir_after_filter = df_ir[cond_ir1 | cond_ir2]

    df_ir_after_filter = pd.merge(
        left=df_ir_after_filter,
        right=df_lookup_bcm_IR,
        left_on="I17COSTREVENUE",
        right_on="I17COSTREVENUE",
        how="left",
    )

    df_ir_after_filter = pd.merge(
        left=df_ir_after_filter,
        right=df_fx_pl,
        left_on="I17ESTCURR",
        right_on="FROM_CURRENCY",
        how="left",
    )

    df_ir_after_filter.loc[:, f"discount_{name1}"] = (
        df_ir_after_filter[f"discount_{name1}_original_CCY"]
        / df_ir_after_filter["EXCHANGE_RATE"]
    )
    df_ir_after_filter.loc[:, f"discount_{name2}"] = (
        df_ir_after_filter[f"discount_{name2}_original_CCY"]
        / df_ir_after_filter["EXCHANGE_RATE"]
    )
    df_ir_after_filter.loc[:, "nominal"] = (
        df_ir_after_filter[f"nominal_original_CCY"]
        / df_ir_after_filter["EXCHANGE_RATE"]
    )

    df_ir_after_filter_grouped = df_ir_after_filter.groupby(
        [
            "account",
            "contract",
            "I17COSTREVENUE",
            "lrc_lic",
            "I17ESTCURR",
        ]
    ).sum()

    df_ir_after_filter_grouped = df_ir_after_filter_grouped.reset_index()
    # negative values
    df_ir_after_filter_grouped.loc[:, "amount"] = df_ir_after_filter_grouped[
        "nominal"
    ].apply(lambda x: -x)

    # df_ir_ccy = rounding_CCY(
    #     df_ir_after_filter_grouped, f"discount_{name1}", f"discount_{name2}"
    # )

    columns_to_show = [
        "account",
        "contract",
        "I17COSTREVENUE",
        "lrc_lic",
        "I17ESTCURR",
        "amount",
        "nominal",
        "nominal_original_CCY",
        f"discount_{name1}",
        f"discount_{name2}",
        f"discount_{name1}_original_CCY",
        f"discount_{name2}_original_CCY",
        # f"npr_{name1}",
        # f"npr_{name2}",
    ]
    df_ir_after_filter_grouped = df_ir_after_filter_grouped[columns_to_show]
    df_ir_after_filter_grouped.to_csv(
        "q1/results/q1_pl/pl_movements_ir.csv", sep=";", decimal=",", index=False
    )

    """
    2)     BCM SM - BCM IR - LRC 
    """

    cond_ir1 = df_ir["lrc_lic"] == "LRC"
    cond_ir2 = df_ir["date_select"] != "Q1"

    df_ir_lcr = df_ir[cond_ir1 & cond_ir2]

    df_ir_lrc_grouped = df_ir_lcr.groupby(
        [
            "contract",
            "I17COSTREVENUE",
            "lrc_lic",
            "I17ESTCURR",
        ]
    )[
        "nominal_original_CCY",
        f"discount_{name3}_original_CCY",
        f"discount_{name4}_original_CCY",
    ].sum()

    df_ir_lrc_grouped = df_ir_lrc_grouped.reset_index()

    cond_sm1 = df_sm["lrc_lic"] == "LRC"
    df_sm_lcr = df_sm[cond_sm1]

    df_sm_lrc_grouped = df_sm_lcr.groupby(
        [
            "contract",
            "I17COSTREVENUE",
            "lrc_lic",
            "I17ESTCURR",
        ]
    )[
        "nominal_original_CCY",
        f"discount_{name3}_original_CCY",
        f"discount_{name4}_original_CCY",
    ].sum()

    df_sm_lrc_grouped = df_sm_lrc_grouped.reset_index()

    df_lrc = pd.merge(
        left=df_sm_lrc_grouped,
        right=df_ir_lrc_grouped,
        left_on=["contract", "I17COSTREVENUE", "lrc_lic", "I17ESTCURR"],
        right_on=["contract", "I17COSTREVENUE", "lrc_lic", "I17ESTCURR"],
        how="outer",
    )
    df_lrc = df_lrc.fillna(0)

    df_lrc = pd.merge(
        left=df_lrc,
        right=df_fx_pl,
        left_on="I17ESTCURR",
        right_on="FROM_CURRENCY",
        how="left",
    )

    df_lrc.loc[:, "amount_original_CCY"] = (
        df_lrc["nominal_original_CCY_x"] - df_lrc["nominal_original_CCY_y"]
    )
    df_lrc.loc[:, "amount"] = df_lrc["amount_original_CCY"] / df_lrc["EXCHANGE_RATE"]
    df_lrc.loc[:, f"discount_{name3}_original_CCY"] = (
        df_lrc[f"discount_{name3}_original_CCY_x"]
        - df_lrc[f"discount_{name3}_original_CCY_y"]
    )
    df_lrc.loc[:, f"discount_{name4}_original_CCY"] = (
        df_lrc[f"discount_{name4}_original_CCY_x"]
        - df_lrc[f"discount_{name4}_original_CCY_y"]
    )
    df_lrc.loc[:, f"discount_{name3}"] = (
        df_lrc[f"discount_{name3}_original_CCY"] / df_lrc["EXCHANGE_RATE"]
    )
    df_lrc.loc[:, f"discount_{name4}"] = (
        df_lrc[f"discount_{name4}_original_CCY"] / df_lrc["EXCHANGE_RATE"]
    )

    # df_lrc_ccy = rounding_CCY(df_lrc, f"discount_{name3}", f"discount_{name4}")

    df_lrc_mapping = pd.merge(
        left=df_lrc,
        right=df_lookup_bcm_IR_SM_lrc,
        left_on="I17COSTREVENUE",
        right_on="I17COSTREVENUE",
        how="left",
    )

    columns_to_show2 = [
        "account",
        "contract",
        "I17COSTREVENUE",
        "lrc_lic",
        "I17ESTCURR",
        "amount",
        "amount_original_CCY",
        f"discount_{name3}",
        f"discount_{name4}",
        f"discount_{name3}_original_CCY",
        f"discount_{name4}_original_CCY",
    ]

    df_lrc_mapping = df_lrc_mapping[columns_to_show2]

    df_lrc_mapping.to_csv(
        "q1/results/q1_pl/pl_movements_sm_lrc.csv", sep=";", decimal=",", index=False
    )

    """
    3)     BCM SM - BCM IR - LIC 
    """

    cond_ir_lic = df_ir["lrc_lic"] == "LIC"
    df_ir_lic = df_ir[cond_ir_lic]

    cond_sm_lic = df_sm["lrc_lic"] == "LIC"
    df_sm_lic = df_sm[cond_sm_lic]

    cond_sm_past = df_sm_lic["date_select"] != "Q1"
    df_sm_lic_past = df_sm_lic[cond_sm_past]
    df_sm_lic_current = df_sm_lic[~cond_sm_past]

    # df_ir_lic
    df_ir_lic_grouped = df_ir_lic.groupby(
        [
            "contract",
            "I17COSTREVENUE",
            "lrc_lic",
            "I17ESTCURR",
        ]
    )[
        "nominal_original_CCY",
        f"discount_{name1}_original_CCY",
        f"discount_{name2}_original_CCY",
    ].sum()

    df_ir_lic_grouped = df_ir_lic_grouped.reset_index()
    #
    # df_sm_lic_past
    df_sm_lic_past_grouped = df_sm_lic_past.groupby(
        [
            "contract",
            "I17COSTREVENUE",
            "lrc_lic",
            "I17ESTCURR",
        ]
    )[
        "nominal_original_CCY",
        f"discount_{name3}_original_CCY",
        f"discount_{name4}_original_CCY",
    ].sum()

    df_sm_lic_past_grouped = df_sm_lic_past_grouped.reset_index()
    #
    # df_sm_lic_current
    df_sm_lic_current_grouped = df_sm_lic_current.groupby(
        [
            "contract",
            "I17COSTREVENUE",
            "lrc_lic",
            "I17ESTCURR",
        ]
    )[
        "nominal_original_CCY",
        f"discount_{name3}_original_CCY",
        f"discount_{name4}_original_CCY",
    ].sum()

    df_sm_lic_current_grouped = df_sm_lic_current_grouped.reset_index()
    #
    df_lic_current = pd.merge(
        left=df_sm_lic_current_grouped,
        right=df_fx_pl,
        left_on="I17ESTCURR",
        right_on="FROM_CURRENCY",
        how="left",
    )

    df_lic_past = pd.merge(
        left=df_sm_lic_past_grouped,
        right=df_ir_lic_grouped,
        left_on=["contract", "I17COSTREVENUE", "lrc_lic", "I17ESTCURR"],
        right_on=["contract", "I17COSTREVENUE", "lrc_lic", "I17ESTCURR"],
        how="outer",
    )
    df_lic_past = df_lic_past.fillna(0)

    df_lic_past = pd.merge(
        left=df_lic_past,
        right=df_fx_pl,
        left_on="I17ESTCURR",
        right_on="FROM_CURRENCY",
        how="left",
    )

    df_lic_past.loc[:, "amount_original_CCY"] = (
        df_lic_past["nominal_original_CCY_x"] - df_lic_past["nominal_original_CCY_y"]
    )
    df_lic_past.loc[:, "amount"] = (
        df_lic_past["amount_original_CCY"] / df_lic_past["EXCHANGE_RATE"]
    )
    df_lic_past.loc[:, f"discount_{name1}_change_original_CCY"] = (
        df_lic_past[f"discount_{name3}_original_CCY"]
        - df_lic_past[f"discount_{name1}_original_CCY"]
    )
    df_lic_past.loc[:, f"discount_{name2}_change_original_CCY"] = (
        df_lic_past[f"discount_{name4}_original_CCY"]
        - df_lic_past[f"discount_{name2}_original_CCY"]
    )

    df_lic_past.loc[:, f"discount_{name1}_change"] = (
        df_lic_past[f"discount_{name1}_change_original_CCY"]
        / df_lic_past["EXCHANGE_RATE"]
    )
    df_lic_past.loc[:, f"discount_{name2}_change"] = (
        df_lic_past[f"discount_{name2}_change_original_CCY"]
        / df_lic_past["EXCHANGE_RATE"]
    )

    # df_lic_past_ccy = rounding_CCY(
    #     df_lic_past, f"discount_{name1}_change", f"discount_{name2}_change"
    # )

    df_lic_mapping_past = pd.merge(
        left=df_lic_past,
        right=df_lookup_bcm_IR_SM_lic_past,
        left_on="I17COSTREVENUE",
        right_on="I17COSTREVENUE",
        how="left",
    )
    columns_to_show_past = [
        "account",
        "contract",
        "I17COSTREVENUE",
        "lrc_lic",
        "I17ESTCURR",
        "amount",
        "amount_original_CCY",
        f"discount_{name1}_change",
        f"discount_{name2}_change",
        f"discount_{name1}_change_original_CCY",
        f"discount_{name2}_change_original_CCY",
    ]
    df_lic_mapping_past = df_lic_mapping_past[columns_to_show_past]

    df_lic_mapping_past.to_csv(
        "q1/results/q1_pl/pl_movements_sm_lic_past.csv",
        sep=";",
        decimal=",",
        index=False,
    )

    """
    lic current
    """
    df_lic_current.loc[:, "amount_original_CCY"] = df_lic_current[
        "nominal_original_CCY"
    ]
    df_lic_current.loc[:, "amount"] = (
        df_lic_current["amount_original_CCY"] / df_lic_current["EXCHANGE_RATE"]
    )

    df_lic_current.loc[:, f"discount_{name3}"] = (
        df_lic_current[f"discount_{name3}_original_CCY"]
        / df_lic_current["EXCHANGE_RATE"]
    )
    df_lic_current.loc[:, f"discount_{name4}"] = (
        df_lic_current[f"discount_{name4}_original_CCY"]
        / df_lic_current["EXCHANGE_RATE"]
    )
    # df_lic_current_ccy = rounding_CCY(
    #     df_lic_current, f"discount_{name3}", f"discount_{name4}"
    # )

    df_lic_mapping_current = pd.merge(
        left=df_lic_current,
        right=df_lookup_bcm_IR_SM_lic_current,
        left_on="I17COSTREVENUE",
        right_on="I17COSTREVENUE",
        how="left",
    )
    columns_to_show_current = [
        "account",
        "contract",
        "I17COSTREVENUE",
        "lrc_lic",
        "I17ESTCURR",
        "amount",
        "amount_original_CCY",
        f"discount_{name3}",
        f"discount_{name4}",
        f"discount_{name3}_original_CCY",
        f"discount_{name4}_original_CCY",
    ]
    df_lic_mapping_current = df_lic_mapping_current[columns_to_show_current]

    df_lic_mapping_current.to_csv(
        "q1/results/q1_pl/pl_movements_sm_lic_current.csv",
        sep=";",
        decimal=",",
        index=False,
    )

    return


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

    # trial()
    # local_portfolio_lookup_table()

    bcm()
    # act()
    # csm()
    # npr(NPR)
    # portfolio_balance()


if __name__ == "__main__":
    started = (
        datetime.datetime.utcnow()
        .replace(tzinfo=datetime.timezone.utc)
        .astimezone(tz=None)
    ).strftime("%H:%M:%S")
    starttime = time.perf_counter()
    print(started)
    main()
    print("Time elapsed:", round(time.perf_counter() - starttime))
