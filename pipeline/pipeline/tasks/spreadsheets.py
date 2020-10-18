import tempfile
from datetime import date
from typing import Tuple

import gspread
import luigi
import numpy
import pandas

from pipeline.config import GSPREAD_CLIENT, WORKSHEET_URL
from pipeline.dashboard_pdf_scrapper import (
    scrap_positive_wards_to_df,
    positive_breakdown_fix_dtypes,
    scrape_case_growth_to_df,
    scrape_elderly_table_df,
    extract_ward_wise_positive_cases_glance_page,
)
from pipeline.extract_history_file import extract_history
from .cities_metrics_v1 import FetchCovid19IndiaDataTask
from .dropbox import textio2binary, textio2stringio
from .stopcoronavirus_mcgm_scrapping import DownloadMcgmDashboardPdfTask


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
        return self.response is not None


class ExtractGlanceWardWisePositiveCases(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())
    page_index = luigi.IntParameter(default=1)
    response = None

    def run(self):
        pdf_input = yield DownloadMcgmDashboardPdfTask(date=self.date)
        worksheet, worksheet_df = worksheet_as_df_by_url(
            WORKSHEET_URL, "ward-wise-positive-cases"
        )
        with pdf_input.open("r") as input_file, tempfile.NamedTemporaryFile(
            "ab+"
        ) as named_tmp_file:
            named_tmp_file.write(textio2binary(input_file))
            scrap_df = extract_ward_wise_positive_cases_glance_page(
                named_tmp_file.name, page_index=self.page_index
            )
            scrap_df["downloaded_for"] = self.date.strftime("%Y-%m-%d")
            result_df = pandas.concat([worksheet_df, scrap_df])
            self.response = worksheet.update(
                [result_df.columns.values.tolist()] + result_df.values.tolist()
            )
            # TODO: Check number of rows written vs scrap_df
            return True
        return False

    def complete(self):
        return self.response is not None


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
        return self.response is not None


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
        return self.response is not None


class ExtractDataFromPdfDashboardGSheetWrapper(luigi.WrapperTask):
    date = luigi.DateParameter(default=date.today())
    # elderly_page = luigi.IntParameter(default=22)
    daily_case_growth_page = luigi.IntParameter(default=23)
    positive_breakdown_index = luigi.IntParameter(default=20)

    def requires(self):
        yield ExtractWardPositiveBreakdownGSheetTask(
            date=self.date, page_index=self.positive_breakdown_index
        )
        yield ExtractCaseGrowthTableGSheetTask(
            date=self.date, page=self.daily_case_growth_page
        )
        # yield ExtractElderlyTableGSheetTask(date=self.date, page=self.elderly_page)


class AllDataGSheetTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=date.today())
    daily_case_growth_page = luigi.IntParameter(default=23)
    positive_breakdown_index = luigi.IntParameter(default=20)
    states_and_districts = luigi.DictParameter()
    glance_page_index = luigi.IntParameter(default=1)

    def requires(self):
        yield ExtractWardPositiveBreakdownGSheetTask(
            date=self.date, page_index=self.positive_breakdown_index
        )
        yield ExtractCaseGrowthTableGSheetTask(
            date=self.date, page=self.daily_case_growth_page
        )
        yield HospitalizationSheetGSheetTask(
            date=self.date, states_and_districts=self.states_and_districts
        )
        yield ExtractGlanceWardWisePositiveCases(
            date=self.date, page_index=self.glance_page_index
        )


class HospitalizationSheetGSheetTask(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())
    states_and_districts = luigi.DictParameter()
    response_metrics = None
    response_hospitalization = None
    response_city_stats = None

    def run(self):
        covid19_api_json_output = yield FetchCovid19IndiaDataTask(date=self.date)
        hospitalization_worksheet, hospitalization_df = worksheet_as_df_by_url(
            WORKSHEET_URL, "hospitalization"
        )
        city_stats_worksheet, city_stats_df = worksheet_as_df_by_url(
            WORKSHEET_URL, "city_stats"
        )
        metrics_worksheet, metrics_df = worksheet_as_df_by_url(WORKSHEET_URL, "metrics")

        tmp_hospitalization = tempfile.NamedTemporaryFile("a+")
        tmp_city_stats = tempfile.NamedTemporaryFile("w+")
        tmp_metrics = tempfile.NamedTemporaryFile("w+")

        with tmp_city_stats as tmp_city_stats_file, (tmp_metrics) as tmp_metrics_file, (
            tmp_hospitalization
        ) as tmp_hospitalization_file, (
            covid19_api_json_output.open("r")
        ) as covid19_json_path:
            hospitalization_df.to_csv(tmp_hospitalization_file)
            # city_stats_df.to_csv(tmp_city_stats_file)
            # metrics_df.to_csv(tmp_metrics_file)

            extract_history(
                textio2stringio(covid19_json_path),
                self.states_and_districts,
                tmp_city_stats_file.name,
                tmp_hospitalization_file.name,
                tmp_metrics_file.name,
            )

            hospitalization_df_update = pandas.read_csv(tmp_hospitalization_file.name)
            city_stats_df_update = pandas.read_csv(tmp_city_stats_file.name)
            metrics_df_update = pandas.read_csv(tmp_metrics_file.name)

            self.response_hospitalization = hospitalization_worksheet.update(
                [hospitalization_df_update.columns.values.tolist()]
                + hospitalization_df_update.replace("", numpy.nan).values.tolist()
            )
            self.response_city_stats = city_stats_worksheet.update(
                [city_stats_df_update.columns.values.tolist()]
                + city_stats_df_update.replace("", numpy.nan).fillna("").values.tolist()
            )
            self.response_metrics = metrics_worksheet.update(
                [metrics_df_update.columns.values.tolist()]
                + [
                    [str(vv).strip() if pandas.notnull(vv) else "" for vv in ll]
                    for ll in metrics_df_update.values.tolist()
                ]
            )

    def complete(self):
        return self.response_metrics is not None
