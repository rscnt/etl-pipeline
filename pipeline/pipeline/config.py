import os

import gspread

from luigi.contrib.dropbox import DropboxClient

DROPBOX_TOKEN = os.getenv("SWB_DROPBOX_TOKEN")
dropbox_client = DropboxClient(DROPBOX_TOKEN)

GSPREAD_CLIENT = gspread.service_account()

WORKSHEET_URL = os.getenv(
    "SWB_WORKSHEET_URL",
    "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0",
)
