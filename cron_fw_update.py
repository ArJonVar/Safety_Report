#region imports
import smartsheet
from smartsheet.exceptions import ApiError
from smartsheet_grid import grid
import requests
import json
import time
from globals import smartsheet_token, bamb_api_key, fw_api_key
from logger import ghetto_logger
import datetime

#endregion

class FwApi():
    '''is meant to work with Safety Mat's Safety Project List to find Active Construction Projects and update the dates that project employees have completed their trainings to have a pulse on the level of training of project teams.'''
    def __init__(self, ss_api_token, fw_api_token):
        self.smartsheet_token=ss_api_token
        self.fw_token=fw_api_key
        grid.token=smartsheet_token
        self.smart = smartsheet.Smartsheet(access_token=self.smartsheet_token)
        self.smart.errors_as_exceptions(True)
        self.safety_pl_sheet_id = 8139053347432324
        self.update_stamp_sum_id = 'get correct id'
        self.start_time = time.time()
        self.log=ghetto_logger("cron_taining_update.py")
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
        self.log.log(f"{self.timestamp()} Successfully connected to Smartsheet")
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
                    print(f"FW link for {item.get('FW')} is incorrect and id could not be extrapolated")
                item["fw_id"] = id
            else:
                item["fw_id"] = "None"
        return data_list
    def locate_posting_column_ids(self):
        '''Generates the Column id's based on column name in case we need to switch sheets for what ever reason'''
        pass
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
    def get_all_forms(self, proj_id):
        '''gets all forms per proj id'''
        url = f"https://app.fieldwire.com/api/v3/projects/{proj_id}/forms"  
        headers = {
            "accept": "application/json",
            "Authorization": "Token api=YIJTTxb2Hb0YNYWf2X01JnAMz3nT9hzdyf23JOhB",
            "Fieldwire-Filter": "active",
            "Fieldwire-Per-Page": "10000"
        }
        response = requests.get(url, headers=headers)
        content = json.loads(response.content)
        return content
    def fetch_fw_data(self, id, url):
        '''manages fw api calls, and fails them at once if needed'''
        try:
            status_dict=self.get_statuses(id)
            template_dict=self.get_templates(id)
            forms_data = self.get_all_forms(id)
            return {"status_dict":status_dict, "template_dict":template_dict, "forms_data":forms_data}
        except AttributeError:
            print(f"error w/ {id} at {url}")
            return{"forms_data":[]}
    def integrate_fw_data(self, data):
        '''integrates all fw data, mapping ids to text (for template/status) and integrating all data with the record per proj'''
        for item in data:
            id = item.get("fw_id")
            if id != "None":
                fw_data = self.fetch_fw_data(id, item.get("FW"))
                
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
    def calc_meta_data(self, forms, template):
        '''returns count and more recent for various templates'''
        filtered_data = []
        for form in forms:
            # filters for Daily Job Log template w/ completed/submitted status
            if form.get('template').find(template) != -1 and (form.get('status').find("Complete") != -1 or form.get('status').find("Submitted") != -1):
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
    def gather_posting_data(self, data):
        '''to gather posting data we need to iterate through row data and find employee ids, and then find each employees trainings. 
        If there is no employee, the value can be "N/A" if an employee is missing a required training the value can be "", and if they have completed it, the value will be their completion date.'''
        self.error = []
        for item in data:
            id = item.get("fw_id")
            forms = item.get("forms")
            if id != "None" and forms != []:
                try:
                    count_daily_joblog,recent_daily_joblog = self.calc_meta_data(item.get('forms'), "Daily Job Log")
                    count_weekly_safetymeeting, recent_weekly_safetymeeting=self.calc_meta_data(item.get('forms'), "Weekly Site Safety Meeting")
                    count_safety_inspections, recent_safety_inspections = self.calc_meta_data(item.get('forms'), "Weekly Site Safety Inspection")
                    count_sssp, recent_sssp = self.calc_meta_data(item.get('forms'), "General Project Info")
                    count_premob, recent_premob= self.calc_meta_data(item.get('forms'), "Pre-mob Sub Checklist")
                    count_phase_review, recent_phase_review = self.calc_meta_data(item.get('forms'), "Project Phase Review")
                    post = {"count_daily_joblog": count_daily_joblog, 
                            "recent_daily_joblog": recent_daily_joblog, 
                            'count_weekly_safetymeeting':count_weekly_safetymeeting,
                            'recent_weekly_safetymeeting':recent_weekly_safetymeeting,
                            'count_safety_inspections': count_safety_inspections,
                            'recent_safety_inspections': recent_safety_inspections,
                            'recent_sssp': recent_sssp,
                            'count_premob': count_premob,
                            'count_phase_review': count_phase_review
                            }
                    item["post"]=post
                except:
                    self.error.append(item)
        return data
    def post_data(self, posting_data):
        pass
    def post_update_stamp(self):
        '''posts date to summary column to tell ppl when the last time this script succeeded was'''
        current_date = datetime.date.today()
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
        df = self.gather_smartsheet_data(self.safety_pl_sheet_id)
        self.df_reordered = self.clean_smartsheet_data(df)
        data = self.gen_ss_data_list(self.df_reordered)
        data=self.integrate_fw_data(data)
        self.posting_data = self.gather_posting_data(data)


if __name__ == "__main__":
    fa = FwApi(smartsheet_token, fw_api_key)
    fa.run()
