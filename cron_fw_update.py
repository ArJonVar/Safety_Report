#region imports
import smartsheet
from smartsheet.exceptions import ApiError
from smartsheet_grid import grid
import requests
import json
import time
from globals import smartsheet_token, fw_api_key
from logger import ghetto_logger
from datetime import datetime, date

#endregion

class FwApi():
    '''is meant to work with Safety Mat's Safety Project List to find Active Construction Projects and update the dates that project employees have completed their trainings to have a pulse on the level of training of project teams.'''
    def __init__(self, ss_api_token, fw_api_token):
        self.smartsheet_token=ss_api_token
        self.fw_token=fw_api_token
        grid.token=smartsheet_token
        self.smart = smartsheet.Smartsheet(access_token=self.smartsheet_token)
        self.smart.errors_as_exceptions(True)
        self.safety_pl_sheet_id = 8139053347432324
        self.update_stamp_sum_id = 8890058333808516
        self.start_time = time.time()
        self.log=ghetto_logger("cron_fw_update.py")
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
        safety_df1 = safety_df.query("`JOB TYPE` == 'Construction' or `JOB TYPE` == 'Small Projects'")
        safety_df2 = safety_df1.query("STATUS == 'Active'")

        safety_df_reordered = safety_df2  [ 
            [
                "id",
                "ENUMERATOR",
                "NAME",
                "FW"

            ]
        ]

        return safety_df_reordered 
    def gen_ss_data_list(self, df):
        '''generates the project id for FW which the FW api needs from the FW link provided, returns a list of data w/fw id'''
        data_list = df.to_dict('records')
        for item in data_list:
            if item.get("FW") != None and item.get("FW") != "None":
                try:
                    # this is always where the id is int he url
                    id = item.get("FW")[38:74]
                except:
                    self.log.log(f"FW link for {item.get('FW')} is incorrect and id could not be extrapolated")
                    item["fw_id"] = "None"
                item["fw_id"] = id
            else:
                item["fw_id"] = "None"
        return data_list
    def locate_posting_column_ids(self):
        '''Generates the Column id's based on column name in case we need to switch sheets for what ever reason'''
        df = self.sheet_columns
        self.count_joblog_columnid = df.loc[df['title'] == 'Count Daily Job Logs']['id'].to_list()[0]
        self.recent_joblog_columnid = df.loc[df['title'] == 'Most Recent Daily Job Log']['id'].to_list()[0]
        self.count_safetymeeting_columnid = df.loc[df['title'] == 'Count Weekly Safety Meetings']['id'].to_list()[0]
        self.recent_safetymeeting_columnid = df.loc[df['title'] == 'Most Recent Weekly Safety Meeting']['id'].to_list()[0]
        self.count_safetyinspections_columnid = df.loc[df['title'] == 'Count Safety Inspections']['id'].to_list()[0]
        self.recent_safetyinspections_columnid = df.loc[df['title'] == 'Most Recent Safety Inspection']['id'].to_list()[0]
        self.recent_geninfo_columnid = df.loc[df['title'] == 'SSSP - General Information']['id'].to_list()[0]
        self.count_premob_columnid = df.loc[df['title'] == 'Total Count SSSP - Pre-Mob Sub Checklists']['id'].to_list()[0]
        self.count_phasereview_columnid = df.loc[df['title'] == 'Total Count SSSP - Phase Reviews']['id'].to_list()[0]
        self.count_photos_columnid = df.loc[df['title'] == 'Count Photos']['id'].to_list()[0]
        self.recent_photo_columnid = df.loc[df['title'] == 'Most Recent Photo']['id'].to_list()[0]
#endregion
#region fw data
    def fw_api_call(self, url):
        '''api call template for basic calls'''
        url = f"https://app.fieldwire.com/api/v3/projects/{url}"  
        headers = {
            "accept": "application/json",
            "Authorization": f"Token api={self.fw_token}"
        }

        response = requests.get(url, headers=headers)
        content = json.loads(response.content)
        return content
    def get_statuses(self, proj_id):
        '''statuses are coded to ids and need to be translated into english per project, here is the api call to do that'''

        api_data = self.fw_api_call(f"{proj_id}/form_template_form_statuses")

        formstatus_dict = {}
        for status in api_data:
            id = status.get('id')
            status = status.get('name')
            formstatus_dict[id] = status

        return formstatus_dict
    def get_templates(self, proj_id):
        '''templates are coded to ids, and need to be translated to english per project'''
        api_data = self.fw_api_call(f"{proj_id}/form_templates")

        template_id_dict = {}
        for temp in api_data:
            id = temp.get('id')
            name = temp.get('name')
            template_id_dict[id] = name


        return template_id_dict
    def fw_api_call_activenall(self, url):
        '''gets all forms per proj id'''
        url = f"https://app.fieldwire.com/api/v3/projects/{url}"  
        headers = {
            "accept": "application/json",
            "Authorization": f"Token api={self.fw_token}",
            "Fieldwire-Filter": "active",
            "Fieldwire-Per-Page": "10000"
        }
        response = requests.get(url, headers=headers)
        content = json.loads(response.content)
        return content
    def fw_api_call_paginated(self, url):
        '''paginates asking for 1000 entries so we get ALL'''
        # Set up initial parameters for API request
        url_paginated = url
        # Retrieve data from the specified API endpoint
        while True:
            # Fetch results from API using current parameters
            results = self.fw_api_call_activenall(url_paginated)
            # Yield each result as it's fetched
            for result in results:
                yield result
            # If there is a next page of data, update parameters for next request
            if len(results) == 1000:
                # Extract the "last_updated_at" value from the last element of the current page
                last_updated_at = results[-1]['updated_at']
                # Update parameters for next page request, including the "last_updated_at" value
                url_paginated = url + f"?last_synced_at={last_updated_at}" 
            else:
                # If there are no more results, break out of the loop
                break
            
    def fetch_fw_data(self, id, url, name):
        '''manages fw api calls, and fails them at once if needed'''
        try:
            status_dict=self.get_statuses(id)
            template_dict=self.get_templates(id)
            forms_data = self.fw_api_call_paginated(f'{id}/forms')
            return {"status_dict":status_dict, "template_dict":template_dict, "forms_data":forms_data}
        except AttributeError:
            self.log.log(f"Need access to {name}: {url}")
            return{"forms_data":[]}
    def integrate_fw_data(self, data):
        '''integrates all fw data, mapping ids to text (for template/status) and integrating all data with the record per proj'''
        for item in data:
            id = item.get("fw_id")
            if id != "None":
                fw_data = self.fetch_fw_data(id, item.get("FW"), item.get("NAME"))
                
                forms = []
                for form in fw_data.get("forms_data"):
                    status_id = form.get("form_template_form_status_id")
                    template_id = form.get("form_template_id")
                    status = fw_data.get("status_dict").get(status_id)
                    template = fw_data.get("template_dict").get(template_id)
                    form_dict = {'id':form.get("id"),
                    'name':form.get('name'),
                    'created_at':form.get("created_at"),
                    'status':status,
                    'template':template
                    }
                    forms.append(form_dict)
                
                item["forms"]=forms

        return data
#endregion
#region post data
    def calc_form_data(self, forms, template_input, template_input_option_2 = "ABCDEFGHIJKLMNOPQRSTUVXYZ"):
        '''returns count and most recent for various templates'''
        filtered_data = []
        for form in forms:
            status = str(form.get('status')).lower()
            template = str(form.get('template')).lower()
            # filters out drafts from Daily Job Log template
            if (template.find(template_input.lower()) != -1 or template.find(template_input_option_2.lower()) != -1) and (status.find("draft") == -1):
                filtered_data.append(form)
        try:
            count = len(filtered_data)
        except:
            count = 0
        try:
            most_recent_date = max(form['created_at'] for form in filtered_data)
        except: 
            most_recent_date = "N/A"

        return count, most_recent_date
    def cal_attachment_data(self, proj_id):
        '''returns count and most recent for (photo) attachments'''
        data = self.fw_api_call_paginated(f'{proj_id}/attachments')
        filtered_data = []
        for attachment in data:
            # filters for Daily Job Log template w/ completed/submitted status
            if attachment.get('kind') == 'photo':
                filtered_data.append(attachment)
        try:
            count = len(filtered_data)
        except:
            count = 0
        try:
            most_recent_date = max(attachment['created_at'] for attachment in filtered_data)
        except: 
            most_recent_date = "N/A"

        return count, most_recent_date
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
        count_daily_joblog,recent_daily_joblog = self.calc_form_data(item.get('forms'), "Daily Job Log", "DJL")
        count_weekly_safetymeeting, recent_weekly_safetymeeting=self.calc_form_data(item.get('forms'), "Safety Meeting")
        count_safety_inspections, recent_safety_inspections = self.calc_form_data(item.get('forms'), "Safety Inspection")
        count_sssp, recent_sssp = self.calc_form_data(item.get('forms'), "General Project")
        count_premob, recent_premob= self.calc_form_data(item.get('forms'), "Pre-mob")
        count_phase_review, recent_phase_review = self.calc_form_data(item.get('forms'), "Phase Review")
        count_photo, recent_photo=self.cal_attachment_data(item.get('fw_id'))
        post = [{"name":"count_daily_joblog", "value":count_daily_joblog, "column_id": self.count_joblog_columnid },
                {"name":"recent_daily_joblog", "value": self.date_parser(recent_daily_joblog), "column_id":self.recent_joblog_columnid},
                {"name":'count_weekly_safetymeeting', "value":count_weekly_safetymeeting, "column_id":self.count_safetymeeting_columnid},
                {"name":'recent_weekly_safetymeeting', "value":self.date_parser(recent_weekly_safetymeeting), "column_id":self.recent_safetymeeting_columnid},
                {"name":'count_safety_inspections', "value":count_safety_inspections, "column_id":self.count_safetyinspections_columnid},
                {"name":'recent_safety_inspections', "value":self.date_parser(recent_safety_inspections), "column_id":self.recent_safetyinspections_columnid},
                {"name":'recent_sssp',  "value":self.date_parser(recent_sssp), "column_id":self.recent_geninfo_columnid},
                {'name':'count_premob', "value":count_premob, "column_id":self.count_premob_columnid},
                {"name":'count_phase_review', "value":count_phase_review, "column_id":self.count_phasereview_columnid},
                {"name":'count_photo', "value":count_photo, 'column_id':self.count_photos_columnid},
                {"name": "recent_photo", "value": self.date_parser(recent_photo), 'column_id':self.recent_photo_columnid}
            ]
        item["post"]=post
        
        return item
    def gather_posting_data(self, data):
        '''to gather posting data we need to iterate through row data and for each row, gather the appropropriate data to prep for posting.'''
        self.error = []
        for item in data:
            id = item.get("fw_id")
            forms = item.get("forms")
            if id != "None":
                try:
                    self.pull_main_data(item)
                except:
                    self.error.append(item)
        
        self.log.log(f"the following {len(self.error)} projects produced an error:")
        for error in self.error:
            self.log.log(f"  {error.get('NAME')}: {error.get('FW')}")
        
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
        data = self.gen_ss_data_list(self.df_reordered)
        self.log.log("pulling fw data")
        data=self.integrate_fw_data(data)
        self.posting_data = self.gather_posting_data(data)
        self.log.log("posting data")
        self.post_data(self.posting_data)
        self.post_update_stamp()
        self.log.log("~fin")



if __name__ == "__main__":
    fa = FwApi(smartsheet_token, fw_api_key)
    fa.run()
