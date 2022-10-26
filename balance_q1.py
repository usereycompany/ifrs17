import datetime
import os
import time
import xdrlib

import pandas as pd

"""


Relevantni je:
    bcm_IR_SM, bcm_IR_SM_Q1 a s nimi spojene preparation_for_groupping

"""

ANALYSIS_DATE = "31.03.2022"
ANALYSIS_DATE_ir = "31.12.2021"
NPR = 0.003


def trial():

    df = pd.read_csv("fpsl_results.csv", delimiter=",", decimal=".")

    group = df.groupby(["G/L Account", "NewGL Segment"])[
        "Amount in Functional Currency"
    ].sum()
    group.to_csv("vysledek.csv", sep=";", decimal=",")
    return


def fpsl_to_csv():

    directory = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "fpsl", "FPSL results")
    )

    columns = [
        "Posting Date",
        "Cost or Revenue Element",
        "NewGL Segment",
        "G/L Account",
        "Amount in Functional Currency",
    ]

    for file in os.listdir(directory):
        whole_path = (
            os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),
                    "fpsl",
                    "FPSL results",
                )
            )
            + "\\"
            + file
        )
        df = pd.read_excel(whole_path)

        final_df_filtered = df[columns]
        name = file[:6]
        print(name)
        final_df_filtered.to_csv(f"fpsl_csv/{name}.csv", sep=";", decimal=",")

    return


def fpsl_results():

    directory_csv = os.path.abspath(os.path.join(os.path.dirname(__file__), "fpsl_csv"))
    directory_excel = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "fpsl", "FPSL results")
    )

    final_df_csv = pd.DataFrame()
    final_df_excel = pd.DataFrame()

    columns = [
        "Posting Date",
        # "Contract/Portfolio",
        # "Assigned Portfolio (Profit Recognition)",
        # "Description Past/Future Service",
        # "Description Loss Component",
        # "NewGL Loss Component",
        # "Contributes to Loss Recovery Component",
        # "Local Portfolio",
        # "Description Process Step ID",
        # "Description Accounting Change",
        "Cost or Revenue Element",
        "NewGL Segment",
        # "Description Cost/Revenue Element",
        # "Transaction Type",
        # "Description Trans. Type",
        # "Description Lifecycle Stage",
        # "Subledger Account",
        # "Description Subledger Acct",
        "G/L Account",
        # "Description G/L Account",
        # "Amount in Transaction Currency",
        # "Transaction Currency",
        "Amount in Functional Currency",
        # "Functional Currency",
        # "Description Onerousness St.",
        # "Description In.Rec.Oner.St.",
    ]

    for file in os.listdir(directory_csv):
        whole_path = directory_csv + "\\" + file
        df = pd.read_csv(whole_path, delimiter=";", decimal=",")
        df = df[columns]
        df_details = df.groupby("G/L Account")["Amount in Functional Currency"].sum()
        df_details = df_details.reset_index()
        final_df_csv = pd.concat([final_df_csv, df_details], ignore_index=True)
        print(file)

    df_csv = final_df_csv.groupby("G/L Account")["Amount in Functional Currency"].sum()
    df_csv = df_csv.reset_index()

    for file in os.listdir(directory_excel):
        whole_path = directory_excel + "\\" + file
        df = pd.read_excel(whole_path)
        df = df[columns]
        df_details = df.groupby("G/L Account")["Amount in Functional Currency"].sum()
        df_details = df_details.reset_index()
        final_df_excel = pd.concat([final_df_excel, df_details], ignore_index=True)
        print(file)

    df_excel = final_df_excel.groupby("G/L Account")[
        "Amount in Functional Currency"
    ].sum()
    df_excel = df_excel.reset_index()

    final_df = pd.merge(
        left=df_csv,
        right=df_excel,
        left_on="G/L Account",
        right_on="G/L Account",
        how="outer",
    )

    final_df.to_csv("fpsl.csv", sep=";", decimal=",")

    return


def fpsl_results2():

    directory = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "fpsl", "FPSL results_v2")
    )
    directory_csv = os.path.abspath(os.path.join(directory, "csv"))
    headers_file = os.path.abspath(os.path.join(directory, "fields.csv"))
    headers = pd.read_csv(headers_file, delimiter=";")

    headers_for_dict = headers.set_index("Field")
    mapping = headers_for_dict.to_dict()
    mapping = mapping["Name"]

    final_df_csv = pd.DataFrame()

    columns = [
        "/BA1/K5SAMBAL",
        "AARK5SAMO",
        "/BA1C/BTTRAMTC",
        "/BA1/BIL_CURR",
        "/BA1C/BTPOSTDT",
        "/WSV/PORTL",
        "/BA1/C55CMETHY",
        "/BA1/C55PROCID",
        "/BA1/C55DOCNUM",
        "/BA1/C55ALST",
        "/BA1/C55DBCDF",
        "/BA1/C55ONRST",
        "AARIGLAC",
        "/BA1/C55YPER",
        "/BA1/C55ONSTIR",
        "AARC55SLA",
        "/WSV/PORTG",
        "/BA1/C55SLACC",
        "AARC35TRX",
        "/BA1/C55CHGRSN",
        "AARC55CON",
        "AARCRCPAY",
        "AARC55LSC",
        "/BA1/C80BUSSG",
        "/WSV/PCLIC",
        "AARC55GIC",
        "/BA1/C55R1CNID",
        "/BA1/C55INSEIN",
        "/BA1/C55LOCOMP",
        "/BA1/C55LORECO",
    ]

    for file in os.listdir(directory_csv):
        whole_path = os.path.abspath(os.path.join(directory_csv, file))
        df = pd.read_csv(whole_path, delimiter=";", names=headers["Field"])
        df = df[columns]
        df = df.rename(columns=mapping)
        final_df_csv = pd.concat([final_df_csv, df], ignore_index=True)
        print(file)

    final_df_csv.loc[:, "Functional Crcy Amt"] = final_df_csv[
        "Functional Crcy Amt"
    ].apply(lambda x: x if x[-1] != "-" else "-" + x[:-1])
    final_df_csv["Functional Crcy Amt"] = final_df_csv["Functional Crcy Amt"].astype(
        float
    )

    final_df_csv.loc[:, "Transaction Crcy Amt"] = final_df_csv[
        "Transaction Crcy Amt"
    ].apply(lambda x: x if x[-1] != "-" else "-" + x[:-1])
    final_df_csv["Transaction Crcy Amt"] = final_df_csv["Transaction Crcy Amt"].astype(
        float
    )

    final_df_csv.to_csv("fpsl_v15_09.csv", sep=";", decimal=",")

    return


def preparation_for_mapping(df: pd.DataFrame):

    df.loc[:, "contract"] = df["I17REFOBJID"]
    df.loc[:, "contract"] = df["contract"].apply(
        lambda x: "_".join(x.split("_")[-3:])
        if "/" not in x
        else "_".join((x.split("/")[0]).split("_")[-3:])
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


def bcm_reduction():

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
    df_df_factors_locked_in_tyc = pd.read_csv(
        "q0/inputs/df.csv", delimiter=";", decimal=","
    )

    df_df_factors_current = df_df_factors_current.rename(
        columns={
            "I17DISCFACT": "I17DISCFACT_current",
        }
    )
    df_df_factors_locked_in_shifted = df_df_factors_locked_in_shifted.rename(
        columns={
            "I17DISCFACT": "I17DISCFACT_locked_in_shifted",
        }
    )
    df_df_factors_locked_in_tyc = df_df_factors_locked_in_tyc.rename(
        columns={
            "I17DISCFACT": "I17DISCFACT_locked_in_tyc",
        }
    )

    df_fx = pd.read_excel("q1/inputs/fx_rates.xlsx")
    df_fx_balance = df_fx[df_fx["type"] == "M"]
    df_fx_pl = df_fx[df_fx["type"] == "AVG"]
    df_fx_balance_q0 = pd.read_excel("q0/inputs/fx_rates.xlsx")

    df_lookup_bcm = pd.read_excel("q1/mapping_tables/lookup_table_bcm.xlsx")
    df_old_contracts = pd.read_excel("q1/mapping_tables/old_contracts.xlsx")
    df_lookup_portfolio = pd.read_excel("q1/mapping_tables/lookup_local_portfolio.xlsx")

    df = df.drop(
        columns=[
            "I17PERIOD",
            "I17TIMEST",
            "I17SOLO",
            "I17FUNCSYSTEM",
            "I17ITEMID",
        ]
    )

    df.loc[:, "settlement"] = pd.to_datetime(df["I17SETTLEMDATE"], format="%Y%m%d")
    df.loc[:, "settlement"] = df["settlement"].dt.date
    df.loc[:, "settlement"] = df["settlement"] + pd.tseries.offsets.MonthEnd(1)
    df.loc[:, "settlement"] = (
        df["settlement"] + pd.tseries.offsets.MonthEnd(-1)
    ).dt.date
    df.loc[:, "date_adjusted"] = df["settlement"]
    df.loc[:, "date_adjusted"] = (
        df["date_adjusted"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )
    df.loc[:, "settlement"] = (
        df["settlement"].apply(lambda x: x.strftime("%Y%m%d")).astype(str)
    )

    df.loc[:, "settlement"] = df["settlement"].apply(
        lambda x: x[0:4]
        if x[0:4] != "2022"
        else "_".join((x.split("/")[0]).split("_")[-2:])
    )

    df.loc[:, "incurred"] = pd.to_datetime(df["I17INCURREDDATE"], format="%Y%m%d")
    df.loc[:, "incurred"] = df["incurred"].dt.date
    df.loc[:, "incurred"] = df["incurred"] + pd.tseries.offsets.MonthEnd(1)
    df.loc[:, "incurred"] = (df["incurred"] + pd.tseries.offsets.MonthEnd(-1)).dt.date
    df.loc[:, "incurred"] = (
        df["incurred"].apply(lambda x: x.strftime("%Y%m%d")).astype(str)
    )

    df.loc[:, "incurred"] = df["incurred"].apply(
        lambda x: x[0:4]
        if x[0:4] != "2022"
        else "_".join((x.split("/")[0]).split("_")[-2:])
    )

    map1 = {
        "20220131": "Q1",
        "20220228": "Q1",
        "20220331": "Q1",
        "20220430": "Q2",
        "20220531": "Q2",
        "20220630": "Q2",
        "20220731": "Q3",
        "20220831": "Q3",
        "20220930": "Q3",
        "20221031": "Q4",
        "20221130": "Q4",
        "20221231": "Q4",
    }

    df = df.replace({"incurred": map1})
    df = df.replace({"settlement": map1})

    df_10 = df[df["I17EXPCHANGE"] == 10]
    df_60 = df[df["I17EXPCHANGE"] == 60]

    df_10 = pd.merge(
        left=df_10,
        right=df_fx_balance_q0[
            [
                "FROM_CURRENCY",
                "EXCHANGE_RATE",
            ]
        ],
        left_on="I17ESTCURR",
        right_on="FROM_CURRENCY",
        how="left",
    )

    df_60 = pd.merge(
        left=df_60,
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

    df = pd.concat([df_10, df_60], ignore_index=True, sort=False)

    df.loc[df["I17ESTCURR"] == "EUR", "EXCHANGE_RATE"] = 1

    df = preparation_for_mapping(df)

    df_df_factors_current.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_factors_current["I17DISCDATE"], format="%d.%m.%Y"
    )

    df_df_factors_current.loc[:, "I17DISCDATE"] = (
        df_df_factors_current["I17DISCDATE"]
        .apply(lambda x: x.strftime("%Y%m%d"))
        .astype(int)
    )

    df = pd.merge(
        left=df,
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

    df = pd.merge(
        left=df,
        right=df_df_factors_locked_in_shifted[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                "I17DISCFACT_locked_in_shifted",
            ]
        ],
        left_on=["date_adjusted", "I17ESTCURR"],
        right_on=["I17DISCDATE", "I17ESTCURR"],
        how="left",
    )

    df = pd.merge(
        left=df,
        right=df_df_factors_locked_in_tyc[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                "I17DISCFACT_locked_in_tyc",
            ]
        ],
        left_on=["date_adjusted", "I17ESTCURR"],
        right_on=["I17DISCDATE", "I17ESTCURR"],
        how="left",
    )

    df = df.drop(
        columns=[
            "I17DISCDATE",
            "I17DISCDATE_x",
            "I17DISCDATE_y",
        ]
    )

    calculation_date = datetime.datetime.date(pd.Timestamp(ANALYSIS_DATE))

    cond1 = df["date_adjusted"] == int(calculation_date.strftime("%Y%m%d"))
    df.loc[
        cond1,
        [
            "I17DISCFACT_locked_in_shifted",
            "I17DISCFACT_current",
            "I17DISCFACT_locked_in_tyc",
        ],
    ] = 1

    df.loc[:, "nominal_original_CCY"] = df["I17ESTAMOUNT"]
    df.loc[:, "nominal"] = df["nominal_original_CCY"] / df["EXCHANGE_RATE"]

    df.loc[:, "dcf_locked_in_shifted_original_CCY"] = (
        df["I17DISCFACT_locked_in_shifted"] * df["nominal_original_CCY"]
    )
    df.loc[:, "dcf_current_original_CCY"] = (
        df["I17DISCFACT_current"] * df["nominal_original_CCY"]
    )
    df.loc[:, "dcf_locked_in_tyc_original_CCY"] = (
        df["I17DISCFACT_locked_in_tyc"] * df["nominal_original_CCY"]
    )

    df.loc[:, "dcf_locked_in"] = (
        df["dcf_locked_in_shifted_original_CCY"] / df["EXCHANGE_RATE"]
    )
    df.loc[:, "dcf_tyc"] = df["dcf_locked_in_tyc_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, "dcf_current"] = df["dcf_current_original_CCY"] / df["EXCHANGE_RATE"]

    df.loc[:, "discount_locked_in"] = df["dcf_locked_in"] - df["nominal"]
    df.loc[:, "discount_tyc"] = df["dcf_tyc"] - df["nominal"]
    df.loc[:, "discount_current"] = df["dcf_current"] - df["nominal"]

    df = df.drop(
        columns=[
            "I17INCURREDDATE",
            "I17SETTLEMDATE",
            "I17ESTAMOUNT",
            "date_adjusted",
            "I17DISCFACT_locked_in_shifted",
            "I17DISCFACT_current",
            "I17DISCFACT_locked_in_tyc",
            "FROM_CURRENCY",
            "EXCHANGE_RATE",
            "date_adjusted",
            "dcf_locked_in_shifted_original_CCY",
            "dcf_locked_in_tyc_original_CCY",
            "dcf_current_original_CCY",
            "dcf_locked_in",
            "dcf_current",
            "dcf_tyc",
        ]
    )

    result = df.groupby(
        [
            "I17REFOBJID",
            "I17EXPCHANGE",
            "I17COSTREVENUE",
            "I17KEYDATE",
            "I17ESTCURR",
            "settlement",
            "incurred",
        ]
    ).sum()

    result.to_csv("q1/results/upravene_bcm_v3.csv", sep=";", decimal=",")

    return result


def preparation_for_groupping(
    df: pd.DataFrame,
    df_fx: pd.DataFrame,
    df_df_1: pd.DataFrame,
    df_df_2: pd.DataFrame,
    df_df_3: pd.DataFrame,
    df_df_4: pd.DataFrame,
    df_df_5: pd.DataFrame,
    name1: str,
    name2: str,
    name3: str,
    name4: str,
    name5: str,
    calc_date: str,
):
    calculation_date = datetime.datetime.date(pd.Timestamp(calc_date))
    df.loc[:, "contract"] = df["I17REFOBJID"]
    # df.loc[:, "contract"] = df["contract"].apply(
    #     lambda x: "_".join(x.split("_")[-3:])
    #     if "/" not in x
    #     else "_".join((x.split("/")[0]).split("_")[-3:])
    # )

    # df.loc[:, "zivot_nezivot"] = df["I17REFOBJID"]
    # df.loc[:, "zivot_nezivot"] = df["zivot_nezivot"].apply(
    #     lambda x: "zivot" if x.split("_")[2][0] == "L" else "nezivot"
    # )

    df.loc[:, "ri_rh"] = df["I17REFOBJID"]
    df.loc[:, "ri_rh"] = df["ri_rh"].apply(lambda x: x.split("_")[2][1:3])

    df = df.drop(
        columns=[
            "I17PERIOD",
            "I17TIMEST",
            "I17SOLO",
            "I17FUNCSYSTEM",
            # "I17EXPCHANGE",
            # "I17KEYDATE",
            # "I17REFOBJID",
            "I17ITEMID",
        ]
    )

    df.loc[:, "settlement"] = pd.to_datetime(df["I17SETTLEMDATE"], format="%Y%m%d")
    df.loc[:, "settlement"] = df["settlement"].dt.date
    df.loc[:, "settlement"] = df["settlement"] + pd.tseries.offsets.MonthEnd(1)
    df.loc[:, "settlement"] = (
        df["settlement"] + pd.tseries.offsets.MonthEnd(-1)
    ).dt.date
    df.loc[:, "date_adjusted"] = df["settlement"]
    df.loc[:, "date_adjusted"] = (
        df["date_adjusted"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )
    df.loc[:, "settlement"] = (
        df["settlement"].apply(lambda x: x.strftime("%Y%m%d")).astype(str)
    )

    df.loc[:, "settlement"] = df["settlement"].apply(
        lambda x: "till 31.12.2021"
        if int(x[0:4]) < 2022
        else ("from 1.1.2023" if int(x[0:4]) > 2022 else x)
    )

    df.loc[:, "incurred"] = pd.to_datetime(df["I17INCURREDDATE"], format="%Y%m%d")
    df.loc[:, "incurred"] = df["incurred"].dt.date
    df.loc[:, "incurred"] = df["incurred"] + pd.tseries.offsets.MonthEnd(1)
    df.loc[:, "incurred"] = (df["incurred"] + pd.tseries.offsets.MonthEnd(-1)).dt.date
    df.loc[:, "incurred"] = (
        df["incurred"].apply(lambda x: x.strftime("%Y%m%d")).astype(str)
    )

    df.loc[:, "incurred"] = df["incurred"].apply(
        lambda x: "till 31.12.2021"
        if int(x[0:4]) < 2022
        else ("from 1.1.2023" if int(x[0:4]) > 2022 else x)
    )

    map1 = {
        "20220131": "Q1",
        "20220228": "Q1",
        "20220331": "Q1",
        "20220430": "Q2",
        "20220531": "Q2",
        "20220630": "Q2",
        "20220731": "Q3",
        "20220831": "Q3",
        "20220930": "Q3",
        "20221031": "Q4",
        "20221130": "Q4",
        "20221231": "Q4",
    }

    df = df.replace({"incurred": map1})
    df = df.replace({"settlement": map1})

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
        right=df_fx[
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
        right=df_df_3[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                f"I17DISCFACT_{name3}",
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

    df = pd.merge(
        left=df,
        right=df_df_4[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                f"I17DISCFACT_{name4}",
            ]
        ],
        left_on=["date_adjusted", "I17ESTCURR"],
        right_on=["I17DISCDATE", "I17ESTCURR"],
        how="left",
    )

    df = pd.merge(
        left=df,
        right=df_df_5[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                f"I17DISCFACT_{name5}",
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
            f"I17DISCFACT_{name3}",
            f"I17DISCFACT_{name4}",
            f"I17DISCFACT_{name5}",
        ],
    ] = 1

    df.loc[:, "nominal_original_CCY"] = df["I17ESTAMOUNT"]
    df.loc[:, "nominal"] = df["nominal_original_CCY"] / df["EXCHANGE_RATE"]

    rounding_exception_CCY1 = ["TWD"]

    cond_exception = df["I17ESTCURR"].isin(rounding_exception_CCY1)
    df.loc[cond_exception, "nominal_original_CCY"] = df["nominal_original_CCY"].apply(
        lambda x: round(x)
    )

    df.loc[:, f"dcf_{name1}_original_CCY"] = (
        df[f"I17DISCFACT_{name1}"] * df["nominal_original_CCY"]
    )
    df.loc[:, f"dcf_{name2}_original_CCY"] = (
        df[f"I17DISCFACT_{name2}"] * df["nominal_original_CCY"]
    )
    df.loc[:, f"dcf_{name3}_original_CCY"] = (
        df[f"I17DISCFACT_{name3}"] * df["nominal_original_CCY"]
    )
    df.loc[:, f"dcf_{name4}_original_CCY"] = (
        df[f"I17DISCFACT_{name4}"] * df["nominal_original_CCY"]
    )
    df.loc[:, f"dcf_{name5}_original_CCY"] = (
        df[f"I17DISCFACT_{name5}"] * df["nominal_original_CCY"]
    )
    rounding_exception_CCY2 = [
        "TWD",
        "HUF",
        "ISK",
        "JPY",
        "KRW",
    ]
    df.loc[:, f"dcf_{name1}"] = df[f"dcf_{name1}_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, f"dcf_{name2}"] = df[f"dcf_{name2}_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, f"dcf_{name3}"] = df[f"dcf_{name3}_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, f"dcf_{name4}"] = df[f"dcf_{name4}_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, f"dcf_{name5}"] = df[f"dcf_{name5}_original_CCY"] / df["EXCHANGE_RATE"]

    df = df.fillna(0)

    cond_exception2 = df["I17ESTCURR"].isin(rounding_exception_CCY2)
    df.loc[cond_exception2, f"dcf_{name1}_original_CCY"] = df[
        f"dcf_{name1}_original_CCY"
    ].apply(lambda x: round(x))
    df.loc[cond_exception2, f"dcf_{name2}_original_CCY"] = df[
        f"dcf_{name2}_original_CCY"
    ].apply(lambda x: round(x))
    df.loc[cond_exception2, f"dcf_{name3}_original_CCY"] = df[
        f"dcf_{name3}_original_CCY"
    ].apply(lambda x: round(x))
    df.loc[cond_exception2, f"dcf_{name4}_original_CCY"] = df[
        f"dcf_{name4}_original_CCY"
    ].apply(lambda x: round(x))
    df.loc[cond_exception2, f"dcf_{name5}_original_CCY"] = df[
        f"dcf_{name5}_original_CCY"
    ].apply(lambda x: round(x))

    df = df.fillna(0)
    df.loc[cond_exception, f"dcf_{name1}"] = df[f"dcf_{name1}"].apply(
        lambda x: round(x)
    )
    df.loc[cond_exception, f"dcf_{name2}"] = df[f"dcf_{name2}"].apply(
        lambda x: round(x)
    )
    df.loc[cond_exception, f"dcf_{name3}"] = df[f"dcf_{name3}"].apply(
        lambda x: round(x)
    )
    df.loc[cond_exception, f"dcf_{name4}"] = df[f"dcf_{name4}"].apply(
        lambda x: round(x)
    )
    df.loc[cond_exception, f"dcf_{name5}"] = df[f"dcf_{name5}"].apply(
        lambda x: round(x)
    )

    df.loc[:, f"discount_{name1}"] = df[f"dcf_{name1}"] - df["nominal"]
    df.loc[:, f"discount_{name2}"] = df[f"dcf_{name2}"] - df["nominal"]
    df.loc[:, f"discount_{name3}"] = df[f"dcf_{name3}"] - df["nominal"]
    df.loc[:, f"discount_{name4}"] = df[f"dcf_{name4}"] - df["nominal"]
    df.loc[:, f"discount_{name5}"] = df[f"dcf_{name5}"] - df["nominal"]

    df.loc[:, f"discount_{name1}_original_CCY"] = (
        df[f"dcf_{name1}_original_CCY"] - df["nominal_original_CCY"]
    )
    df.loc[:, f"discount_{name2}_original_CCY"] = (
        df[f"dcf_{name2}_original_CCY"] - df["nominal_original_CCY"]
    )
    df.loc[:, f"discount_{name3}_original_CCY"] = (
        df[f"dcf_{name3}_original_CCY"] - df["nominal_original_CCY"]
    )
    df.loc[:, f"discount_{name4}_original_CCY"] = (
        df[f"dcf_{name4}_original_CCY"] - df["nominal_original_CCY"]
    )
    df.loc[:, f"discount_{name5}_original_CCY"] = (
        df[f"dcf_{name5}_original_CCY"] - df["nominal_original_CCY"]
    )

    return df


def discount_npr(
    df: pd.DataFrame,
    df_fx: pd.DataFrame,
    df_df_1: pd.DataFrame,
    df_df_2: pd.DataFrame,
    df_df_3: pd.DataFrame,
    df_df_4: pd.DataFrame,
    name1: str,
    name2: str,
    name3: str,
    name4: str,
):

    df.loc[:, "contract"] = df["I17REFOBJID"]
    # df.loc[:, "contract"] = df["contract"].apply(
    #     lambda x: "_".join(x.split("_")[-3:])
    #     if "/" not in x
    #     else "_".join((x.split("/")[0]).split("_")[-3:])
    # )

    # df.loc[:, "zivot_nezivot"] = df["I17REFOBJID"]
    # df.loc[:, "zivot_nezivot"] = df["zivot_nezivot"].apply(
    #     lambda x: "zivot" if x.split("_")[2][0] == "L" else "nezivot"
    # )

    df.loc[:, "ri_rh"] = df["I17REFOBJID"]
    df.loc[:, "ri_rh"] = df["ri_rh"].apply(lambda x: x.split("_")[2][1:3])

    df = df.drop(
        columns=[
            "I17PERIOD",
            "I17TIMEST",
            "I17SOLO",
            "I17FUNCSYSTEM",
            # "I17EXPCHANGE",
            # "I17KEYDATE",
            # "I17REFOBJID",
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
        right=df_fx[
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
        right=df_df_3[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                f"I17DISCFACT_{name3}",
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

    df = pd.merge(
        left=df,
        right=df_df_4[
            [
                "I17DISCDATE",
                "I17ESTCURR",
                f"I17DISCFACT_{name4}",
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
            f"I17DISCFACT_{name3}",
            f"I17DISCFACT_{name4}",
        ],
    ] = 1

    df.loc[:, "nominal_original_CCY"] = df["I17ESTAMOUNT"]
    df.loc[:, "nominal"] = df["nominal_original_CCY"] / df["EXCHANGE_RATE"]

    df.loc[:, f"dcf_{name1}_original_CCY"] = (
        df[f"I17DISCFACT_{name1}"] * df["nominal_original_CCY"]
    )
    df.loc[:, f"dcf_{name2}_original_CCY"] = (
        df[f"I17DISCFACT_{name2}"] * df["nominal_original_CCY"]
    )
    df.loc[:, f"dcf_{name3}_original_CCY"] = (
        df[f"I17DISCFACT_{name3}"] * df["nominal_original_CCY"]
    )
    df.loc[:, f"dcf_{name4}_original_CCY"] = (
        df[f"I17DISCFACT_{name4}"] * df["nominal_original_CCY"]
    )

    df.loc[:, f"dcf_{name1}"] = df[f"dcf_{name1}_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, f"dcf_{name2}"] = df[f"dcf_{name2}_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, f"dcf_{name3}"] = df[f"dcf_{name3}_original_CCY"] / df["EXCHANGE_RATE"]
    df.loc[:, f"dcf_{name4}"] = df[f"dcf_{name4}_original_CCY"] / df["EXCHANGE_RATE"]

    df.loc[:, f"discount_{name1}"] = df[f"dcf_{name1}"] - df["nominal"]
    df.loc[:, f"discount_{name2}"] = df[f"dcf_{name2}"] - df["nominal"]
    df.loc[:, f"discount_{name3}"] = df[f"dcf_{name3}"] - df["nominal"]
    df.loc[:, f"discount_{name4}"] = df[f"dcf_{name4}"] - df["nominal"]

    df.loc[:, f"discount_{name1}_original_CCY"] = (
        df[f"dcf_{name1}_original_CCY"] - df["nominal_original_CCY"]
    )
    df.loc[:, f"discount_{name2}_original_CCY"] = (
        df[f"dcf_{name2}_original_CCY"] - df["nominal_original_CCY"]
    )
    df.loc[:, f"discount_{name3}_original_CCY"] = (
        df[f"dcf_{name3}_original_CCY"] - df["nominal_original_CCY"]
    )
    df.loc[:, f"discount_{name4}_original_CCY"] = (
        df[f"dcf_{name4}_original_CCY"] - df["nominal_original_CCY"]
    )

    return df


def bcm_IR_SM():

    # df_life = pd.read_csv("q1/inputs/bcm_life.csv", delimiter=";", decimal=",")

    df = pd.read_csv("q0/inputs/bcm.csv", delimiter=";", decimal=",")
    # df = pd.read_csv("q0/inputs/bcm.csv", delimiter=";", decimal=",", nrows=100_000)
    df_sm = pd.read_csv("q1/inputs/bcm_v3.csv", delimiter=";", decimal=",")
    # df_sm = pd.read_csv(
    #     "q1/inputs/bcm_v3.csv", delimiter=";", decimal=",", nrows=100_000
    # )

    # df = pd.concat([df, df_life], ignore_index=True, sort=False)

    # df_df_factors_current = pd.read_csv(
    #     "q1/inputs/df_current.csv", delimiter=";", decimal=","
    # )
    df_df_1 = pd.read_csv("q1/inputs/df/df1.csv", delimiter=";", decimal=",")
    df_df_2 = pd.read_csv("q1/inputs/df/df2.csv", delimiter=";", decimal=",")
    df_df_3 = pd.read_csv("q1/inputs/df/df3.csv", delimiter=";", decimal=",")
    df_df_4 = pd.read_csv("q1/inputs/df/df4.csv", delimiter=";", decimal=",")

    name1 = "tyc"
    name2 = "ycu_31_12"
    name3 = "tyc_shift"
    name4 = "ycu_31_12_shift"
    name_dummy = "dummy"

    df_df_1 = df_df_1.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name1}"})
    df_df_2 = df_df_2.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name2}"})
    df_df_3 = df_df_3.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name3}"})
    df_df_4 = df_df_4.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name4}"})

    df_df_1.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_1["I17DISCDATE"], format="%Y%m%d"
    )

    df_df_1.loc[:, "I17DISCDATE"] = (
        df_df_1["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_3.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_3["I17DISCDATE"], format="%Y%m%d"
    )

    df_df_3.loc[:, "I17DISCDATE"] = (
        df_df_3["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_2.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_2["I17DISCDATE"], format="%d.%m.%Y"
    )

    df_df_2.loc[:, "I17DISCDATE"] = (
        df_df_2["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_4.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_4["I17DISCDATE"], format="%d.%m.%Y"
    )
    df_df_4.loc[:, "I17DISCDATE"] = (
        df_df_4["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    """
    Dummy df
    """
    df_df_5 = df_df_4.copy()
    df_df_5 = df_df_5.rename(
        columns={f"I17DISCFACT_{name4}": f"I17DISCFACT_{name_dummy}"}
    )

    df_fx = pd.read_excel("q0/inputs/fx_rates.xlsx")

    df_10 = df_sm[df_sm["I17EXPCHANGE"] == 10]

    df = preparation_for_groupping(
        df,
        df_fx,
        df_df_1,
        df_df_2,
        df_df_3,
        df_df_4,
        df_df_5,
        name1,
        name2,
        name3,
        name4,
        name_dummy,
        ANALYSIS_DATE_ir,
    )
    df_10 = preparation_for_groupping(
        df_10,
        df_fx,
        df_df_1,
        df_df_2,
        df_df_3,
        df_df_4,
        df_df_5,
        name1,
        name2,
        name3,
        name4,
        name_dummy,
        ANALYSIS_DATE_ir,
    )

    result_original_df = df.groupby(
        [
            "contract",
            "I17EXPCHANGE",
            "I17COSTREVENUE",
            "I17KEYDATE",
            "settlement",
            "incurred",
            "I17ESTCURR",
        ]
    ).sum()

    result_ir_df = df_10.groupby(
        [
            "contract",
            "I17EXPCHANGE",
            "I17COSTREVENUE",
            "I17KEYDATE",
            "settlement",
            "incurred",
            "I17ESTCURR",
        ]
    ).sum()

    result = pd.merge(
        left=result_original_df,
        right=result_ir_df,
        left_on=[
            "contract",
            "I17EXPCHANGE",
            "I17COSTREVENUE",
            "I17KEYDATE",
            "settlement",
            "incurred",
            "I17ESTCURR",
        ],
        right_on=[
            "contract",
            "I17EXPCHANGE",
            "I17COSTREVENUE",
            "I17KEYDATE",
            "settlement",
            "incurred",
            "I17ESTCURR",
        ],
        how="outer",
    )

    result = result.fillna(0)

    result.loc[:, "nominal"] = result["nominal_x"] + result["nominal_y"]
    result.loc[:, "nominal_original_CCY"] = (
        result["nominal_original_CCY_x"] + result["nominal_original_CCY_y"]
    )
    result.loc[:, f"discount_{name1}"] = (
        result[f"discount_{name1}_x"] + result[f"discount_{name1}_y"]
    )
    result.loc[:, f"discount_{name2}"] = (
        result[f"discount_{name2}_x"] + result[f"discount_{name2}_y"]
    )
    result.loc[:, f"discount_{name3}"] = (
        result[f"discount_{name3}_x"] + result[f"discount_{name3}_y"]
    )
    result.loc[:, f"discount_{name4}"] = (
        result[f"discount_{name4}_x"] + result[f"discount_{name4}_y"]
    )

    result.loc[:, f"discount_{name1}_original_CCY"] = (
        result[f"discount_{name1}_original_CCY_x"]
        + result[f"discount_{name1}_original_CCY_y"]
    )
    result.loc[:, f"discount_{name2}_original_CCY"] = (
        result[f"discount_{name2}_original_CCY_x"]
        + result[f"discount_{name2}_original_CCY_y"]
    )
    result.loc[:, f"discount_{name3}_original_CCY"] = (
        result[f"discount_{name3}_original_CCY_x"]
        + result[f"discount_{name3}_original_CCY_y"]
    )
    result.loc[:, f"discount_{name4}_original_CCY"] = (
        result[f"discount_{name4}_original_CCY_x"]
        + result[f"discount_{name4}_original_CCY_y"]
    )

    result = result.reset_index()

    """
    NPR
    """
    result.loc[:, "I17COSTREVENUE"] = result["I17COSTREVENUE"].astype("string")

    codes_applicable = [
        "Y103",
        "Y204",
        "Y220",
    ]
    cond = result["I17COSTREVENUE"].isin(codes_applicable)

    result.loc[:, f"npr_{name1}"] = 0
    result.loc[cond, f"npr_{name1}"] = (
        (result["nominal"] + result[f"discount_{name1}"]) * -1 * NPR
    )

    result.loc[:, f"npr_{name2}"] = 0
    result.loc[cond, f"npr_{name2}"] = (
        (result["nominal"] + result[f"discount_{name2}"]) * -1 * NPR
    )

    columns_to_show = [
        "contract",
        "I17EXPCHANGE",
        "I17COSTREVENUE",
        "I17KEYDATE",
        "settlement",
        "incurred",
        "I17ESTCURR",
        "nominal",
        "nominal_original_CCY",
        f"discount_{name1}",
        f"discount_{name2}",
        f"discount_{name3}",
        f"discount_{name4}",
        f"discount_{name1}_original_CCY",
        f"discount_{name2}_original_CCY",
        f"discount_{name3}_original_CCY",
        f"discount_{name4}_original_CCY",
        f"npr_{name1}",
        f"npr_{name2}",
    ]
    result = result[columns_to_show]

    """
    oprava Q1 CFs pouzivajicich shift - vychazelo 0, tak upravuju na discount bez shiftu
    """
    cond_shift = result["settlement"] == "Q1"
    result.loc[cond_shift, f"discount_{name3}"] = result[f"discount_{name1}"]
    result.loc[cond_shift, f"discount_{name4}"] = result[f"discount_{name2}"]
    result.loc[cond_shift, f"discount_{name3}_original_CCY"] = result[
        f"discount_{name1}_original_CCY"
    ]
    result.loc[cond_shift, f"discount_{name4}_original_CCY"] = result[
        f"discount_{name2}_original_CCY"
    ]

    final = result.groupby(
        [
            "contract",
            "I17EXPCHANGE",
            "I17COSTREVENUE",
            "I17KEYDATE",
            "I17ESTCURR",
            "settlement",
            "incurred",
        ]
    ).sum()

    final.to_csv("q1/results/ir/result_bcm_ir_v3.csv", sep=";", decimal=",")

    return result


def bcm_IR_SM_Q1():

    # df_life = pd.read_csv("q1/inputs/bcm_life.csv", delimiter=";", decimal=",")

    df_sm = pd.read_csv("q1/inputs/bcm_v3.csv", delimiter=";", decimal=",")
    # df_sm = pd.read_csv(
    #     "q1/inputs/bcm_v3.csv", delimiter=";", decimal=",", nrows=100_000
    # )
    # df = pd.concat([df, df_life], ignore_index=True, sort=False)

    # df_df_factors_current = pd.read_csv(
    #     "q1/inputs/df_current.csv", delimiter=";", decimal=","
    # )
    df_df_1 = pd.read_csv("q1/inputs/df/df1.csv", delimiter=";", decimal=",")
    df_df_2 = pd.read_csv("q1/inputs/df/df2.csv", delimiter=";", decimal=",")
    df_df_3 = pd.read_csv("q1/inputs/df/df3.csv", delimiter=";", decimal=",")
    df_df_4 = pd.read_csv("q1/inputs/df/df4.csv", delimiter=";", decimal=",")
    df_df_5 = pd.read_csv("q1/inputs/df/df5.csv", delimiter=";", decimal=",")

    name1 = "tyc"
    name2 = "ycu_31_12"
    name3 = "tyc_shift"
    name4 = "ycu_31_12_shift"
    name5 = "ycu_31_03"

    df_df_1 = df_df_1.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name1}"})
    df_df_2 = df_df_2.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name2}"})
    df_df_3 = df_df_3.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name3}"})
    df_df_4 = df_df_4.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name4}"})
    df_df_5 = df_df_5.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name5}"})

    df_df_1.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_1["I17DISCDATE"], format="%Y%m%d"
    )

    df_df_1.loc[:, "I17DISCDATE"] = (
        df_df_1["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_2.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_2["I17DISCDATE"], format="%d.%m.%Y"
    )

    df_df_2.loc[:, "I17DISCDATE"] = (
        df_df_2["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_3.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_3["I17DISCDATE"], format="%Y%m%d"
    )

    df_df_3.loc[:, "I17DISCDATE"] = (
        df_df_3["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_4.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_4["I17DISCDATE"], format="%d.%m.%Y"
    )
    df_df_4.loc[:, "I17DISCDATE"] = (
        df_df_4["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_5.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_5["I17DISCDATE"], format="%d.%m.%Y"
    )
    df_df_5.loc[:, "I17DISCDATE"] = (
        df_df_5["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_fx = pd.read_excel("q1/inputs/fx_rates.xlsx")
    df_fx_balance = df_fx[df_fx["type"] == "M"]

    df_60 = df_sm[df_sm["I17EXPCHANGE"] == 60]

    df_60 = preparation_for_groupping(
        df_60,
        df_fx_balance,
        df_df_1,
        df_df_2,
        df_df_3,
        df_df_4,
        df_df_5,
        name1,
        name2,
        name3,
        name4,
        name5,
        ANALYSIS_DATE_ir,
    )

    result = df_60.groupby(
        [
            "contract",
            "I17EXPCHANGE",
            "I17COSTREVENUE",
            "I17KEYDATE",
            "settlement",
            "incurred",
            "I17ESTCURR",
        ]
    ).sum()

    result = result.reset_index()

    """
    NPR
    """
    result.loc[:, "I17COSTREVENUE"] = result["I17COSTREVENUE"].astype("string")

    codes_applicable = [
        "Y103",
        "Y204",
        "Y220",
    ]
    cond = result["I17COSTREVENUE"].isin(codes_applicable)

    result.loc[:, f"npr_{name5}"] = 0
    result.loc[cond, f"npr_{name5}"] = (
        (result["nominal"] + result[f"discount_{name5}"]) * -1 * NPR
    )

    """
    oprava Q1 CFs pouzivajicich shift - vychazelo 0, tak upravuju na discount bez shiftu
    """
    cond_shift = result["settlement"] == "Q1"
    result.loc[cond_shift, f"discount_{name3}"] = result[f"discount_{name1}"]
    result.loc[cond_shift, f"discount_{name4}"] = result[f"discount_{name2}"]
    result.loc[cond_shift, f"discount_{name3}_original_CCY"] = result[
        f"discount_{name1}_original_CCY"
    ]
    result.loc[cond_shift, f"discount_{name4}_original_CCY"] = result[
        f"discount_{name2}_original_CCY"
    ]

    columns_to_show = [
        "contract",
        "I17COSTREVENUE",
        "nominal_original_CCY",
        "nominal",
        f"discount_{name3}",
        f"discount_{name4}",
        f"discount_{name5}",
        f"discount_{name3}_original_CCY",
        f"discount_{name4}_original_CCY",
        f"discount_{name5}_original_CCY",
        f"npr_{name5}",
        "I17EXPCHANGE",
        "I17KEYDATE",
        "I17ESTCURR",
        "settlement",
        "incurred",
    ]
    result = result[columns_to_show]

    final = result.groupby(
        [
            "contract",
            "I17EXPCHANGE",
            "I17COSTREVENUE",
            "I17KEYDATE",
            "settlement",
            "incurred",
            "I17ESTCURR",
        ]
    ).sum()

    final.to_csv("q1/results/ir/result_bcm_sm_v3.csv", sep=";", decimal=",")

    return result


def check_on_no():

    # df_life = pd.read_csv("q1/inputs/bcm_life.csv", delimiter=";", decimal=",")
    df_sm = pd.read_csv("q1/inputs/bcm.csv", delimiter=";", decimal=",")
    # df_sm = pd.read_csv("q1/inputs/bcm.csv", delimiter=";", decimal=",", nrows=100_000)

    df_df_1 = pd.read_csv("q1/inputs/df/df1.csv", delimiter=";", decimal=",")
    df_df_2 = pd.read_csv("q1/inputs/df/df2.csv", delimiter=";", decimal=",")
    df_df_3 = pd.read_csv("q1/inputs/df/df3.csv", delimiter=";", decimal=",")
    df_df_4 = pd.read_csv("q1/inputs/df/df4.csv", delimiter=";", decimal=",")

    name1 = "tyc"
    name2 = "ycu_31_12"
    name3 = "tyc_shift"
    name4 = "ycu_31_12_shift"

    df_df_1 = df_df_1.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name1}"})
    df_df_2 = df_df_2.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name2}"})
    df_df_3 = df_df_3.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name3}"})
    df_df_4 = df_df_4.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name4}"})

    df_df_1.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_1["I17DISCDATE"], format="%Y%m%d"
    )

    df_df_1.loc[:, "I17DISCDATE"] = (
        df_df_1["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_3.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_3["I17DISCDATE"], format="%Y%m%d"
    )

    df_df_3.loc[:, "I17DISCDATE"] = (
        df_df_3["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_2.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_2["I17DISCDATE"], format="%d.%m.%Y"
    )

    df_df_2.loc[:, "I17DISCDATE"] = (
        df_df_2["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_4.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_4["I17DISCDATE"], format="%d.%m.%Y"
    )
    df_df_4.loc[:, "I17DISCDATE"] = (
        df_df_4["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_fx_balance = pd.read_excel("q0/inputs/fx_rates.xlsx")

    df_10 = df_sm[df_sm["I17EXPCHANGE"] == 10]

    df = preparation_for_groupping(
        df_10,
        df_fx_balance,
        df_df_1,
        df_df_2,
        df_df_3,
        df_df_4,
        name1,
        name2,
        name3,
        name4,
    )

    df = preparation_for_mapping(df)

    result = df.groupby(
        [
            "ri_rh",
            "contract",
            "I17COSTREVENUE",
        ]
    ).sum()

    result = result.reset_index()
    """
    NPR
    """
    result.loc[:, "I17COSTREVENUE"] = result["I17COSTREVENUE"].astype("string")

    codes_applicable = [
        "Y103",
        "Y204",
        "Y220",
    ]
    cond = result["I17COSTREVENUE"].isin(codes_applicable)
    result.loc[:, f"npr_{name1}"] = 0
    result.loc[cond, f"npr_{name1}"] = (
        (result["nominal"] + result[f"discount_{name1}"]) * -1 * NPR
    )

    """
    
    
    
    Upravit NPR -- melo by byt asi 1-2 a DF 1-4, coz je 8 kombinaci
    
    
    
    
    """
    result.loc[:, f"csm_{name3}"] = (
        result["nominal"] + result[f"discount_{name3}"] + result[f"npr_{name5}"]
    )
    result.loc[:, f"csm_{name4}"] = (
        result["nominal"] + result[f"discount_{name4}"] + result[f"npr_{name5}"]
    )
    result.loc[:, f"csm_{name5}"] = (
        result["nominal"] + result[f"discount_{name5}"] + result[f"npr_{name5}"]
    )

    final = result.groupby(["ri_rh", "contract"]).sum()

    check_df = final.reset_index()
    check_df.loc[:, "on_no"] = check_df["contract"].apply(lambda x: x.split("_")[1])

    cond_on1 = check_df["ri_rh"] == "RI"
    cond_on23 = check_df[f"csm_{name3}"] < 0
    cond_on24 = check_df[f"csm_{name4}"] < 0
    cond_on25 = check_df[f"csm_{name5}"] < 0

    check_df.loc[:, f"on_no_should_be_{name3}"] = "NO"
    check_df.loc[:, f"on_no_should_be_{name4}"] = "NO"
    check_df.loc[:, f"on_no_should_be_{name5}"] = "NO"
    check_df.loc[cond_on1 & cond_on23, f"on_no_should_be_{name3}"] = "ON"
    check_df.loc[cond_on1 & cond_on24, f"on_no_should_be_{name4}"] = "ON"
    check_df.loc[cond_on1 & cond_on25, f"on_no_should_be_{name5}"] = "ON"

    cond_check_1 = check_df[f"on_no_should_be_{name3}"] != check_df["on_no"]
    cond_check_2 = check_df[f"on_no_should_be_{name4}"] != check_df["on_no"]
    cond_check_3 = check_df[f"on_no_should_be_{name5}"] != check_df["on_no"]

    final = check_df[cond_check_1 | cond_check_2 | cond_check_3]

    columns = [
        "ri_rh",
        "contract",
        f"csm_{name3}",
        f"csm_{name4}",
        f"csm_{name5}",
        "on_no",
        f"on_no_should_be_{name3}",
        f"on_no_should_be_{name4}",
        f"on_no_should_be_{name5}",
    ]
    final = final[columns]

    final.to_csv("q1/results/ir/check_on_no.csv", sep=";", decimal=",")

    return final


def bcm_IR_SM_10_ungrouped():

    # df_life = pd.read_csv("q1/inputs/bcm_life.csv", delimiter=";", decimal=",")
    # df = pd.read_csv("q0/inputs/bcm.csv", delimiter=";", decimal=",")
    # df = pd.read_csv("q0/inputs/bcm.csv", delimiter=";", decimal=",", nrows=100_000)
    df_sm = pd.read_csv("q1/inputs/bcm.csv", delimiter=";", decimal=",")
    # df_sm = pd.read_csv("q1/inputs/bcm.csv", delimiter=";", decimal=",", nrows=100_000)
    # df = pd.concat([df, df_life], ignore_index=True, sort=False)

    # df_df_factors_current = pd.read_csv(
    #     "q1/inputs/df_current.csv", delimiter=";", decimal=","
    # )
    df_df_1 = pd.read_csv("q1/inputs/df/df1.csv", delimiter=";", decimal=",")
    df_df_2 = pd.read_csv("q1/inputs/df/df2.csv", delimiter=";", decimal=",")
    df_df_3 = pd.read_csv("q1/inputs/df/df3.csv", delimiter=";", decimal=",")
    df_df_4 = pd.read_csv("q1/inputs/df/df4.csv", delimiter=";", decimal=",")

    name1 = "tyc"
    name2 = "ycu_31_12"
    name3 = "tyc_shift"
    name4 = "ycu_31_12_shift"

    df_df_1 = df_df_1.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name1}"})
    df_df_2 = df_df_2.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name2}"})
    df_df_3 = df_df_3.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name3}"})
    df_df_4 = df_df_4.rename(columns={"I17DISCFACT": f"I17DISCFACT_{name4}"})

    df_df_1.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_1["I17DISCDATE"], format="%Y%m%d"
    )

    df_df_1.loc[:, "I17DISCDATE"] = (
        df_df_1["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_3.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_3["I17DISCDATE"], format="%Y%m%d"
    )

    df_df_3.loc[:, "I17DISCDATE"] = (
        df_df_3["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_2.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_2["I17DISCDATE"], format="%d.%m.%Y"
    )

    df_df_2.loc[:, "I17DISCDATE"] = (
        df_df_2["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_df_4.loc[:, "I17DISCDATE"] = pd.to_datetime(
        df_df_4["I17DISCDATE"], format="%d.%m.%Y"
    )
    df_df_4.loc[:, "I17DISCDATE"] = (
        df_df_4["I17DISCDATE"].apply(lambda x: x.strftime("%Y%m%d")).astype(int)
    )

    df_fx = pd.read_excel("q0/inputs/fx_rates.xlsx")

    df_10 = df_sm[df_sm["I17EXPCHANGE"] == 10]

    result = discount_npr(
        df_10,
        df_fx,
        df_df_1,
        df_df_2,
        df_df_3,
        df_df_4,
        name1,
        name2,
        name3,
        name4,
    )

    """
    NPR
    """
    result.loc[:, "I17COSTREVENUE"] = result["I17COSTREVENUE"].astype("string")

    codes_applicable = [
        "Y103",
        "Y204",
        "Y220",
    ]
    cond = result["I17COSTREVENUE"].isin(codes_applicable)

    result.loc[:, f"npr_{name1}"] = 0
    result.loc[cond, f"npr_{name1}"] = (
        (result["nominal"] + result[f"discount_{name1}"]) * -1 * NPR
    )

    result.loc[:, f"npr_{name2}"] = 0
    result.loc[cond, f"npr_{name2}"] = (
        (result["nominal"] + result[f"discount_{name2}"]) * -1 * NPR
    )

    columns_to_show = [
        "contract",
        "I17EXPCHANGE",
        "I17COSTREVENUE",
        "I17KEYDATE",
        "I17INCURREDDATE",
        "I17SETTLEMDATE",
        "I17ESTCURR",
        "nominal",
        "nominal_original_CCY",
        f"discount_{name1}",
        f"discount_{name2}",
        f"discount_{name3}",
        f"discount_{name4}",
        f"discount_{name1}_original_CCY",
        f"discount_{name2}_original_CCY",
        f"discount_{name3}_original_CCY",
        f"discount_{name4}_original_CCY",
        f"npr_{name1}",
        f"npr_{name2}",
    ]
    result = result[columns_to_show]

    final = result
    # final = result.groupby(
    #     [
    #         "contract",
    #         "I17EXPCHANGE",
    #         "I17COSTREVENUE",
    #         "I17KEYDATE",
    #         "I17ESTCURR",
    #         "settlement",
    #         "incurred",
    #     ]
    # ).sum()

    final.to_csv("q1/results/ir/result_bcm_ir10_ungrouped.csv", sep=";", decimal=",")

    return result


def main():

    # trial()
    # fpsl_to_csv()
    # fpsl_results()
    # fpsl_results2()
    # bcm_reduction()
    bcm_IR_SM()
    bcm_IR_SM_Q1()
    # check_on_no()
    # bcm_IR_SM_10_ungrouped()


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
