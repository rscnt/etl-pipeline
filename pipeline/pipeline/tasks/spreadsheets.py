from datetime import date
from typing import Tuple
import tempfile
import datetime

import luigi
import pandas
import gspread


from pipeline.dashboard_pdf_scrapper import (
    scrap_positive_wards_to_df,
    scrape_elderly_table,
    positive_breakdown_fix_dtypes,
    scrape_case_growth_to_df,
    scrape_elderly_table_df,
)

from pipeline.config import GSPREAD_CLIENT, WORKSHEET_URL

from .dropbox import dropbox_target, textio2binary, textio2stringio
from .stopcoronavirus_mcgm_scrapping import DownloadMcgmDashboardPdfTask
from .cities_metrics_v1 import FetchCovid19IndiaDataTask
from .cities_metrics import calculate_metrics_input


def worksheet_as_df_by_url(
    sheet_url: str, worksheet_name: str
) -> Tuple[gspread.Worksheet, pandas.DataFrame]:
    sheet = GSPREAD_CLIENT.open_by_url(sheet_url)
    worksheet = sheet.worksheet(worksheet_name)
    return worksheet, pandas.DataFrame(worksheet.get_all_records())


class ExtractWardPositiveBreakdownGSheetTask(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())
    page_index = luigi.IntParameter(default=22)
    response = None

    def run(self):
        pdf_input = yield DownloadMcgmDashboardPdfTask(date=self.date)
        worksheet, positive_breakdown_df = worksheet_as_df_by_url(
            WORKSHEET_URL, "positive-breakdown"
        )
        positive_breakdown_df = positive_breakdown_fix_dtypes(positive_breakdown_df)
        with pdf_input.open("r") as input_file, tempfile.NamedTemporaryFile(
            "ab+"
        ) as named_tmp_file:
            named_tmp_file.write(textio2binary(input_file))
            scrap_df = scrap_positive_wards_to_df(named_tmp_file, page=self.page_index)
            scrap_df["downloaded_for"] = self.date.strftime("%Y-%m-%d")
            result_df = pandas.concat([positive_breakdown_df, scrap_df])
            self.response = worksheet.update(
                [result_df.columns.values.tolist()] + result_df.values.tolist()
            )
            # TODO: Check number of rows written vs scrap_df
            return True
        return False

    def complete(self):
        return self.response != None


class ExtractCaseGrowthTableGSheetTask(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())
    page = luigi.IntParameter(default=25)
    response = None

    def run(self):
        pdf_input = yield DownloadMcgmDashboardPdfTask(date=self.date)
        worksheet, daily_case_growth_df = worksheet_as_df_by_url(
            WORKSHEET_URL, "daily-case-growth"
        )

        with pdf_input.open("r") as input_file, tempfile.NamedTemporaryFile(
            "ab+"
        ) as named_tmp_file:
            named_tmp_file.write(textio2binary(input_file))
            scrap_df = scrape_case_growth_to_df(named_tmp_file.name, page=self.page)
            scrap_df["downloaded_for"] = self.date.strftime("%Y-%m-%d")
            result_df = pandas.concat([daily_case_growth_df, scrap_df])
            self.response = worksheet.update(
                [result_df.columns.values.tolist()] + result_df.values.tolist()
            )
            return True
        return False

    def complete(self):
        return self.response != None


class ExtractElderlyTableGSheetTask(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())
    page = luigi.IntParameter(default=22)
    response = None

    def run(self):
        pdf_input = yield DownloadMcgmDashboardPdfTask(date=self.date)
        worksheet, elderly_df = worksheet_as_df_by_url(WORKSHEET_URL, "elderly")

        with pdf_input.open("r") as input_file, tempfile.NamedTemporaryFile(
            "ab+"
        ) as named_tmp_file:
            named_tmp_file.write(textio2binary(input_file))
            scrap_df = scrape_elderly_table_df(named_tmp_file.name, page=self.page)
            scrap_df["downloaded_for"] = self.date.strftime("%Y-%m-%d")
            result_df = pandas.concat([elderly_df, scrap_df])
            result_df = result_df.fillna("NA")
            self.response = worksheet.update(
                [result_df.columns.values.tolist()] + result_df.values.tolist()
            )
            return True
        return False

    def complete(self):
        return self.response != None


class ExtractDataFromPdfDashboardGSheetWrapper(luigi.WrapperTask):
    date = luigi.DateParameter(default=date.today())
    elderly_page = luigi.IntParameter(default=22)
    daily_case_growth_page = luigi.IntParameter(default=25)
    positive_breakdown_index = luigi.IntParameter(default=22)

    def requires(self):
        yield ExtractWardPositiveBreakdownGSheetTask(
            date=self.date, page_index=self.positive_breakdown_index
        )
        yield ExtractCaseGrowthTableGSheetTask(
            date=self.date, page=self.daily_case_growth_page
        )
        yield ExtractElderlyTableGSheetTask(date=self.date, page=self.elderly_page)
