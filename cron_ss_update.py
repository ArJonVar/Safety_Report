#region imports
import smartsheet
from smartsheet.exceptions import ApiError
from smartsheet_grid import grid
import requests
import json
import time
from globals import smartsheet_token, smartsheet_token_admin
from logger import ghetto_logger
from datetime import datetime, date

#endregion

class SsApi():
    '''is meant to work with Safety Mat's Safety Project List to find Active Construction Projects and update the dates that project employees have completed their trainings to have a pulse on the level of training of project teams.'''
    def __init__(self, ss_api_token, ss_api_token_admin):
        self.smartsheet_token=ss_api_token
        self.smartsheet_token_admin = ss_api_token_admin
        grid.token=smartsheet_token
        self.smart = smartsheet.Smartsheet(access_token=self.smartsheet_token)
        self.admin_smart = smartsheet.Smartsheet(access_token=self.smartsheet_token_admin)
        self.smart.errors_as_exceptions(True)
        self.safety_pl_sheet_id = 8139053347432324
        self.update_stamp_sum_id = 'get new sum id'
        self.start_time = time.time()
        self.log=ghetto_logger("cron_ss_update.py")
        self.get_all_workspaces()
    def timestamp(self): 
        '''creates a string of minute/second from start_time until now for logging'''
        end_time = time.time()  # get the end time of the program
        elapsed_time = end_time - self.start_time  # calculate the elapsed time in seconds       

        minutes, seconds = divmod(elapsed_time, 60)  # convert to minutes and seconds       
        timestamp = "{:02d}:{:02d}".format(int(minutes), int(seconds))
        
        return timestamp
#region get smartsheet data
    def gather_smartsheet_data(self, sheet_id):
        '''uses grid class to gather all data from sheet'''
        sheet = grid(sheet_id)
        sheet.fetch_content()
        self.sheet_columns = sheet.get_column_df()
        self.locate_posting_column_ids()
        self.log.log(f"Successfully connected to Smartsheet")
        return sheet.df
    def clean_smartsheet_data(self, safety_df):
        '''filters the data for the parameters we need, and then only returns columns we need'''
        safety_df1 = safety_df.query("`JOB TYPE` == 'Construction'")
        safety_df2 = safety_df1.query("STATUS == 'Active'")

        safety_df_reordered = safety_df2  [ 
            [
                "id",
                "ENUMERATOR",
                "NAME",
                "SS"

            ]
        ]

        return safety_df_reordered 
    
    def locate_posting_column_ids(self):
        '''Generates the Column id's based on column name in case we need to switch sheets for what ever reason'''
        df = self.sheet_columns
        self.recent_sis_columnid = df.loc[df['title'] == 'Most Recent Update to SIS 3 Week Look Ahead']['id'].to_list()[0]
    def get_all_workspaces(self):
        response = self.admin_smart.Workspaces.list_workspaces(include_all=True)
        self.wkspc_data=response.to_dict().get('data')
    def gen_ss_data_list(self, df):
        '''generates the workspace id from link, and reformats data to list of dictionaries'''
        data_list = df.to_dict('records')
        for item in data_list:
            if item.get("SS") != None and item.get("FW") != "None":
                try:
                    for wksc in self.wkspc_data:
                        if wksc.get("permalink") == item.get("SS"):
                            id = wksc.get('id')
                            break
                    item["ss_id"] = id
                except:
                    self.log.log(f"SS link for {item.get('SS')} is incorrect and id could not be extrapolated")
                    item["ss_id"] = "None"
            else:
                item["ss_id"] = "None"
        return data_list
#endregion
#region post data
    def date_parser(self, input_str):
        '''changes the date from the format it gets in fw {'2023-03-27T19:21:51.879Z'} to the format easiest on the eyes {%m/%d/%y}'''
        if input_str != "N/A":
            # Parse the input string using datetime.strptime
            dt = datetime.strptime(input_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            # Format the datetime object as a string in the desired format
            formatted_date = dt.strftime('%Y-%m-%d')
            # Return the formatted string
            return formatted_date
        else:
            return ""
    def pull_main_data(self, item):
        '''the main data pull, which grabs all needed data, and formats it correct'''
        recent_sis="blah"
        post = [{"name":"recent sis", "value":recent_sis, "column_id": self.recent_sis_columnid },
            ]
        item["post"]=post
        
        return item
    def search_inside_schedule_folder(self, wksc_id):
        '''looks for a schedule folder, and if one exists, checks for sis by passing folder id to checking function'''
        response = self.admin_smart.Workspaces.list_folders(
          wksc_id,       # workspace_id
          include_all=True)
        
        folder_id = 'none'
        if response.to_dict().get('data') != None:
            for folder in response.to_dict().get('data'):
                if folder.get("name").find('schedule') != -1 or folder.get("name").find('Schedule') != -1:
                    folder = self.admin_smart.Folders.get_folder(folder.get('id'))
                    fold_sheets = folder.to_dict().get("sheets")
                    self.check_sheets_for_sis(fold_sheets)

    def check_sheets_for_sis(self, sheets):
        '''logic for checking for sis sheet and returning it'''
        if sheets != None:
            for sheet in sheets:
                sheet_name = sheet.get("name")
                if sheet_name.find('week') != -1 or sheet_name.find('Week') != -1 or sheet_name.find('WEEK') != -1:
                    print("---", sheet_name)

    def find_sis(self, wksc_id):
        '''looks for the sis file that we are trying to access'''
        print(wksc_id)
        self.search_inside_schedule_folder(wksc_id)
        workspace = self.admin_smart.Workspaces.get_workspace(wksc_id) 
        sheets = workspace.to_dict().get("sheets")
        self.check_sheets_for_sis(sheets)

    def gather_posting_data(self, data):
        '''to gather posting data we need to iterate through row data and for each row, gather the appropropriate data to prep for posting.'''
        self.error = []
        for item in data:
            id = item.get("ss_id")
            if id != "None":
                # try:
                self.find_sis(id)
                    # self.pull_main_data(item)
                # except:
                #     self.error.append(item)
        
        self.log.log(f"the following {len(self.error)} projects produced an error:")
        for error in self.error:
            self.log.log(f"  {error.get('NAME')}: {error.get('SS')}")
        
        return data
    def post_data(self, posting_data):
        '''posting to ss'''
        self.log.log(f"posting...")
        self.row_data = []
        for i, row in enumerate(posting_data):
            if row.get("post") != None:
                new_row = smartsheet.models.Row()
                new_row.id = int(row.get("id"))
                for column in row.get("post"):
                    column_id = column.get("column_id")
                    value = column.get("value")
                    new_cell = smartsheet.models.Cell()
                    new_cell.column_id = int(column_id)
                    new_cell.value = value
                    new_cell.strict = False
                    new_row.cells.append(new_cell)
                    if new_row not in self.row_data:
                        self.row_data.append(new_row)
        resp = self.smart.Sheets.update_rows(
            int(self.safety_pl_sheet_id),      # sheet_id
            self.row_data)
        
        self.log.log(resp.message)
    def post_update_stamp(self):
        '''posts date to summary column to tell ppl when the last time this script succeeded was'''
        current_date = date.today()
        formatted_date = current_date.strftime('%m/%d/%y')

        sum = smartsheet.models.SummaryField({
            "id": self.update_stamp_sum_id,
            "ObjectValue":formatted_date
        })

        resp = self.smart.Sheets.update_sheet_summary_fields(
            self.safety_pl_sheet_id,    # sheet_id
            [sum],
            False    # rename_if_conflict
        )
#endregion
    def run(self):
        self.log.log("pulling ss data")
        df = self.gather_smartsheet_data(self.safety_pl_sheet_id)
        self.df_reordered = self.clean_smartsheet_data(df)
        self.data = self.gen_ss_data_list(self.df_reordered)
        self.posting_data = self.gather_posting_data(self.data)
        self.log.log("posting data")
        # self.post_data(self.posting_data)
        # self.post_update_stamp()
        # self.log.log("~fin")



if __name__ == "__main__":
    ss = SsApi(smartsheet_token, smartsheet_token_admin)
    ss.run()
