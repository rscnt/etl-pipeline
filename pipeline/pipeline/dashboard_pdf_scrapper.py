"""
This file just organizes what's inside the PDF_scrape_text.py module.

We assume that we are scrapping the following PDF:

- http://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf

Which is updated every now and then with updated information.

We intend to scrap the numbers for positives cases in the 
"Ward-wise breakdown of positive cases" where we assume that 
the elements to scrap are in the page 23 inside the bunding box at:
(x0: 295.2, top: 79.2, x1: 770.4, bottom: 504.0).

An ilustration on the are we scrapping can be seen in the image: ./section-to-scrap.png
"""

import re
import logging
from datetime import datetime

import pandas as pd
import numpy as np
import pdfplumber as pdfplumber
import tabula
from tabula.io import read_pdf as tabula_read_pdf


def read_pdf(source_file_path):
    return pdfplumber.open(source_file_path)


def extract_breakdown_positive_cases_date(positive_cases_page):
    return positive_cases_page.extract_text().split("\n")[1]


def extract_wards_data_from_page(positive_cases_page):
    top_left_x = 295.2  # points
    top_left_y = 79.2  # points
    size_width = 475.2  # points
    size_height = 424.8  # points
    bbox = (top_left_x, top_left_y, top_left_x + size_width, top_left_y + size_height)
    page_bbox = positive_cases_page.within_bbox(bbox)
    wards_lines = map(
        lambda ward_line: ward_line.split(), page_bbox.extract_text().split("\n")
    )
    result = [
        extract_ward_data_from_pdf_line(ward_pdf_line) for ward_pdf_line in wards_lines
    ]
    return result


def extract_ward_data_from_pdf_line(ward_pdf_line):
    return {
        "Ward_Name": ward_pdf_line[0],
        "Total_Positive_Cases": ward_pdf_line[1],
        "Total_Discharged": ward_pdf_line[2],
        "Total_Deaths": ward_pdf_line[3],
        "Total_Active": ward_pdf_line[4],
    }


def postive_breakdown_pdf_columns():
    return [
        "as_of",
        "Ward_Name",
        "Total_Positive_Cases",
        "Total_Discharged",
        "Total_Deaths",
        "Total_Active",
        "imputed",
    ]


def pdf_data_to_pandas_df(ward_pdf_info):
    ward_positive_df = pd.DataFrame(
        ward_pdf_info,
        columns=postive_breakdown_pdf_columns(),
    )
    ward_positive_df["Total_Positive_Cases"] = pd.to_numeric(
        ward_positive_df["Total_Positive_Cases"]
    )
    ward_positive_df["Total_Deaths"] = pd.to_numeric(ward_positive_df["Total_Deaths"])
    ward_positive_df["Total_Discharged"] = pd.to_numeric(
        ward_positive_df["Total_Discharged"]
    )
    ward_positive_df["Total_Active"] = pd.to_numeric(ward_positive_df["Total_Active"])
    return ward_positive_df


def export_df_to_file(df, output_ward_path):
    return df.to_csv(output_ward_path, index=False)


def positive_breakdown_fix_dtypes(ward_positive_df):
    if ward_positive_df.empty:
        return pd.DataFrame([], columns=postive_breakdown_pdf_columns())
    ward_positive_df["Total_Positive_Cases"] = pd.to_numeric(
        ward_positive_df["Total_Positive_Cases"]
    )
    ward_positive_df["Total_Deaths"] = pd.to_numeric(ward_positive_df["Total_Deaths"])
    ward_positive_df["Total_Discharged"] = pd.to_numeric(
        ward_positive_df["Total_Discharged"]
    )
    ward_positive_df["Total_Active"] = pd.to_numeric(ward_positive_df["Total_Active"])
    ward_positive_df["imputed"] = pd.to_numeric(ward_positive_df["imputed"])
    return ward_positive_df.copy()


def scrap_positive_wards_to_df(source_file_path, page=20):
    pdf = read_pdf(source_file_path)
    positive_cases_page = pdf.pages[page]
    breakdown_date_string = extract_breakdown_positive_cases_date(positive_cases_page)
    ward_pdf_data = extract_wards_data_from_page(positive_cases_page)
    ward_positive_df = pdf_data_to_pandas_df(ward_pdf_data)
    breakdown_date = datetime.strptime(breakdown_date_string, "As of %b %d, %Y")
    ward_positive_df["as_of"] = breakdown_date.strftime("%Y-%m-%d")
    ward_positive_df["imputed"] = 0
    return ward_positive_df.copy()


def scrap_positive_wards_to_csv(source_file_path, output_path, page=22):
    ward_positive_df = scrap_positive_wards_to_df(source_file_path, page)
    ward_positive_df.to_csv(output_path, index=False)


def scrape_case_growth_to_csv(source_file_path, output_path, page=25):
    new_cases = scrape_case_growth_to_df(source_file_path, page)
    new_cases.to_csv(output_path)


# scrapping case growth
def scrape_case_growth_to_df(source_file_path, page=25):
    new_case_growth = tabula_read_pdf(
        source_file_path, pages=page, multiple_tables=False
    )
    new_cases = new_case_growth[0]

    # check that correct table was scraped
    nc_expected_header = [
        "Date of report",
        "RC",
        "HW",
        "RS",
        "RN",
        "PS",
        "A",
        "C",
        "D",
        "KW",
        "T",
        "PN",
        "N",
        "FN",
        "FS",
        "MW",
        "ME",
        "B",
        "E",
        "GS",
        "KE",
        "GN",
        "S",
        "HE",
        "L",
        "Grand Total",
    ]

    try:
        new_cases.columns == nc_expected_header
    except ValueError:
        logging.error(f"Incorrect columns in ward cases table page={page}")

    new_cases = new_cases[nc_expected_header]

    row_11_values = new_cases.loc[11, nc_expected_header[1:]]
    if not row_11_values.isnull().all():  # value for each ward should be null
        logging.error(f"Unexpected values in row 11 page={page}")

    for row in range(1, 11):
        if not np.array_equal(
            new_cases.loc[row, nc_expected_header[1:-1]],
            new_cases.loc[row, nc_expected_header[1:-1]].astype(str),
        ):  # value for each ward should be a str
            logging.error(f"Unexpected values in rows 1-10 page={page}")

    # clean table
    new_cases.drop(labels=11, inplace=True)

    # save ward identifier with corresponding ward name in a dictionary
    # wards = new_cases.iloc[0][1:] # series of ward names, index is identifiers
    # identifiers = new_cases.iloc[0][1:].index
    # ward_identifiers = {}
    # wards.drop('Grand Total', inplace=True)

    # for ward in wards:
    #     text = str(ward)
    #     name = text.replace('\r',' ')
    #     index = wards[wards==text].index[0]
    #     ward_identifiers[index] = name

    # len(ward_identifiers) == len(wards) # check that dictionary has all wards

    new_cases.drop(labels=0, inplace=True)
    # save as csv files
    # saved_files_path = '/Users/wasilaq/SWB/data-analysis/'

    return new_cases.copy()


def scrape_elderly_table(source_file_path, output_path, page=23):
    elderly_df = scrape_elderly_table_df(source_file_path, page=23)
    elderly_df.to_csv(output_path)


def scrape_elderly_table_df(source_file_path, page=23):
    # elderly screening data
    elderly_screening = tabula_read_pdf(
        source_file_path, pages=page, multiple_tables=False
    )
    elderly = elderly_screening[0]

    # check that correct table was scraped
    e_expected_header = [
        "Wards",
        "Total No. of Houses",
        "Population",
        "Total No. of",
        "Sr Citizen",
        "* Sr Citizen",
        "Unnamed: 6",
        "Unnamed: 7",
    ]
    if not (elderly.columns == e_expected_header).all():
        logging.error(f"Incorrect columns in elderly table elderly_page={page}")

    row_1_values = elderly.loc[1, e_expected_header[1:]]
    if not row_1_values.isnull().all():  # value for each ward should be null
        logging.error(f"Unexpected values in row 1 elderly_page={page}")

    for column in e_expected_header[:4]:
        if not np.array_equal(elderly[column][2:], elderly[column][2:].astype(str)):
            logging.error(f"Unexpected values in f{column} elderly_page={page}")

    if not np.array_equal(
        elderly["Unnamed: 6"][2:], elderly["Unnamed: 6"][2:].astype(float)
    ):
        logging.error(f"Unexpected values in {column}")

    # clean table
    elderly.loc[2, "Wards"] = "Daily Totals"
    elderly.drop(labels=[0, 1], inplace=True)
    elderly.drop(columns=["* Sr Citizen", "Unnamed: 7"], inplace=True)
    elderly.columns = [
        "Wards",
        "Total No. of Houses Surveyed",
        "Population Covered",
        "Total No. of Senior Citizens",
        "Sr Citizen SPO2>95",
        "Sr Citizen SPO2<95",
    ]
    elderly.index = elderly["Wards"]
    # elderly.drop(columns=["Wards"], inplace=True)
    return elderly.copy()
