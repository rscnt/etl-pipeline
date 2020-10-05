import os
import logging

import gspread

from luigi.contrib.dropbox import DropboxClient

DROPBOX_TOKEN = os.getenv("SWB_DROPBOX_TOKEN")
dropbox_client = DropboxClient(DROPBOX_TOKEN)

GSPREAD_CLIENT = None
DEFAULT_WORKSHEET_URL = "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"


WORKSHEET_URL = os.getenv("SWB_WORKSHEET_URL", DEFAULT_WORKSHEET_URL)

try:
    GSPREAD_CLIENT = gspread.service_account()
except FileNotFoundError as e:
    logging.error(f"Unable to create gspread client #{e}")
