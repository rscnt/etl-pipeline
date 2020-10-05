"""Uses a CSV file with the list of existing dates on which to run the pipeline.tasks.spreadsheets.ExtractDataFromPdfDashboardGSheetWrapper

The CSV should have the following columns (in order):

- date-of-pdf: the date that appers in the PDF name
- elderly-table-page: the page of the elderly table
- positive-breakdown-page: the page of the positive breakdown table
- case-growth-page:	the page of the daily case growth table

We are going to be running the ExtractDataFromPdfDashboardGSheetWrapper task for each
entry in the CSV.
"""
import argparse

import pandas
import luigi

from pipeline.tasks.spreadsheets import ExtractDataFromPdfDashboardGSheetWrapper


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reads the previous PDFs with pages CSV URL."
    )
    parser.add_argument(
        "csv_uri",
        type=str,
        help="The URL or Path to the CSV with the argument list for ExtractDataFromPdfDashboardGSheetWrapper",
    )
    csv_uri = parser.parse_args().csv_uri
    print(f"CSV URI {csv_uri}")
    # pandas#read_csv manages the download and parsing of values.
    task_args_df = pandas.read_csv(csv_uri)
    task_args_df["date-of-pdf"] = pandas.to_datetime(task_args_df["date-of-pdf"])
    # the positive breakdown page task uses the page index instead of the page number.
    task_args_df["positive-breakdown-page"] = (
        task_args_df["positive-breakdown-page"] - 1
    )
    task_list = []
    for index, row in task_args_df.iterrows():
        task_list.append(
            ExtractDataFromPdfDashboardGSheetWrapper(
                date=row["date-of-pdf"],
                elderly_page=row["elderly-table-page"],
                daily_case_growth_page=row["case-growth-page"],
                positive_breakdown_index=row["positive-breakdown-page"],
            )
        )
    # We assume we don't have a luigi deamon running.
    luigi_run_result = luigi.build(
        task_list, local_scheduler=True, detailed_summary=True
    )
    print(luigi_run_result.summary_text)
