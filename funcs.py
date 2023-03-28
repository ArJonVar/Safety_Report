#region imports
import smartsheet
from smartsheet.exceptions import ApiError
from smartsheet_grid import grid
import requests
import json
import time
from globals import smartsheet_token, sensative_egnyte_token
from logger import ghetto_logger
import datetime
import requests
from requests.structures import CaseInsensitiveDict
#endregion

class NewClass():
    '''Explain Class'''
    def __init__(self, config):
        self.config = config
        self.smartsheet_token=config.get('smartsheet_token')
        self.sheet_id=config.get("sheet_id")
        grid.token=smartsheet_token
        self.smart = smartsheet.Smartsheet(access_token=self.smartsheet_token)
        self.smart.errors_as_exceptions(True)
        self.log=ghetto_logger("funcs.py")
        self.now = datetime.datetime.now()
    
    def export_sheet(self, file_path):
        '''grabs sheet w/id, and exports it to path'''
        sheet = grid(self.sheet_id)
        sheet.fetch_content()
        sheet.df.to_csv(file_path, index=False)

    def upload_tocloud(self, file):
        '''puts file on egnyte'''
        url = f"https://dowbuilt.egnyte.com/pubapi/v1/fs-content/Shared/Digital%20Construction%20Team/03_DCT%20Admin/Historical%20Data/Projects_%26_Planning_Data/{self.now.strftime('%Y.%m.%d.%H.%S')}_dct-p&p.xlsx"   
        headers = CaseInsensitiveDict()
        headers["Authorization"] = f"Bearer {sensative_egnyte_token}"
        headers["Content-Length"] = "0" 
        with open(f"/home/ariel/dct_admindata_automation/backups/{self.now.strftime('%Y.%m.%d.%H.%S')}_dct-PnP.csv", 'rb') as f:
            data = f.read()
        resp = requests.post(url, headers=headers, data=data)  
        self.log.log(resp.status_code)

    def run(self):
        '''runs main script as intended'''
        self.log.log('ran script')
        path = f"backups/{self.now.strftime('%Y.%m.%d.%H.%S')}_dct-PnP.csv"
        self.export_sheet(path)
        self.log.log(path)

if __name__ == "__main__":
    config = {
        'smartsheet_token':smartsheet_token,
        'sheet_id':2789557075765124
    }
    nc = NewClass(config)
    nc.run()