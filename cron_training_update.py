#region imports
import smartsheet
from smartsheet.exceptions import ApiError
from smartsheet_grid import grid
import requests
import json
import time
from globals import smartsheet_token, bamb_api_key
from logger import ghetto_logger
import datetime

#endregion

class TrainingUpdater():
    '''is meant to work with Safety Mat's Safety Project List to find Active Construction Projects and update the dates that project employees have completed their trainings to have a pulse on the level of training of project teams.'''
    def __init__(self, ss_api_token, bamb_api_token):
        self.smartsheet_token=ss_api_token
        self.bamb_token=bamb_api_key
        grid.token=smartsheet_token
        self.smart = smartsheet.Smartsheet(access_token=self.smartsheet_token)
        self.smart.errors_as_exceptions(True)
        self.safety_pl_sheet_id = 8139053347432324
        self.update_stamp_sum_id = 7628464453904260
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
        sheet = grid(sheet_id)
        sheet.fetch_content()
        # sheet_name=sheet.grid_name
        self.sheet_columns = sheet.get_column_df()
        # sheet_duplicate = grid(sheet_id)
        # sheet_duplicate.fetch_summary_content()
        # sheet_sum = sheet_duplicate.df
        self.log.log(f"{self.timestamp()} Successfully connected to Smartsheet")
        return sheet.df
    def clean_smartsheet_data(self, safety_df):
        safety_df1 = safety_df.query("`JOB TYPE` == 'Construction'")
        safety_df2 = safety_df1.query("STATUS == 'Active'")

        safety_df_reordered = safety_df2  [ 
            [
                "id",
                "ENUMERATOR",
                "Superintendent OSHA Training Complete",
                "Superintendent First Aid/CPR Complete",
                "Super Employee #",
                "PM OSHA Training Complete",
                "PM Employee #",
                "PE OSHA Training Complete",
                "PE Employee #",
                "FM OSHA Training Complete",
                "FM First Aid/CPR Complete",
                "FM Employee #"

            ]
        ]

        return safety_df_reordered 
    def locate_posting_column_ids(self):
        '''Generates the Column id's based on column name in case we need to switch sheets for what ever reason'''
        # self.sheet_columns.query("title == 'NAME'")['id'].to_list()[0]
        # self.sheet_columns
        pass
#endregion
#region get bamboo data
    def bamb_get_training(self):
        url = "https://api.bamboohr.com/api/gateway.php/Dowbuilt/v1/training/type"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers, auth=(self.bamb_token, ''))
        self.training_list = json.loads(response.content.decode('utf-8'))
        self.log.log(f"{self.timestamp()} Successfully connected to BambooHR")
    def bamb_get_employee_training(self, num):
        url = f"https://api.bamboohr.com/api/gateway.php/dowbuilt/v1/training/record/employee/{int(num)}"

        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers, auth=(bamb_api_key, ''))

        employee = json.loads(response.content.decode('utf-8'))

        if employee == []:
            employee_training_portfolio = []

        else:
            employee_trainings = list(employee.keys())

            employee_training_portfolio = []
            for training_id in employee_trainings:
                training_type_instance = employee.get(training_id).get("type")
                training_name_instance = self.training_list.get(training_type_instance).get('name')
                employee_training_portfolio.append([training_name_instance, employee.get(training_id).get("completed")])
        
        return employee_training_portfolio
    def get_employee_handler(self, num, type, id):
        try:
            if num.find("N/A") == -1:
                return self.bamb_get_employee_training(int(num))
            else:
                return "N/A"
        except:
            error = f"{type} prob at id: {id} w/ employee #: {num}"
            self.log.log(error)
            return error
#endregion
#region post data
    def gather_posting_data(self, data_dict):
        '''to gather posting data we need to iterate through row data and find employee ids, and then find each employees trainings. 
        If there is no employee, the value can be "N/A" if an employee is missing a required training the value can be "", and if they have completed it, the value will be their completion date.'''
        posting_data = []       
        self.log.log(f"{self.timestamp()} gathering posting data...")
        for record in data_dict:
            sup = record.get('Super Employee #')
            pm = record.get('PM Employee #')
            pe = record.get('PE Employee #')
            fm = record.get('FM Employee #')
            enum=record.get('ENUMERATOR')   
            #region GENERATING CONDITIONAL POSTING DATA PER JOB TITLE
            result_sup = self.get_employee_handler(sup, 'sup', enum)
            if sup.find("N/A") == -1:
                sup_osha = ""
                sup_cpr = ""
            else:
                sup_osha = "N/A"
                sup_cpr = "N/A"
            for result in result_sup:
                if result[0] == "OSHA Training":
                    if sup_osha == "" or sup_osha == "N/A":
                        sup_osha = result[1]
                    elif result[1] > sup_osha:
                        sup_osha = result[1]
                if result[0] == 'First Aid/CPR':
                    if sup_cpr == "" or sup_cpr == "N/A":
                        sup_cpr = result[1]
                    elif result[1] > sup_cpr:
                        sup_cpr = result[1]

            result_pm = self.get_employee_handler(pm, 'pm', enum)
            if pm.find("N/A") == -1:
                pm_osha=""
            else:
                pm_osha="N/A"
            for result in result_pm:
                if result[0] == "OSHA Training":
                    if pm_osha == "" or pm_osha == "N/A":
                        pm_osha = result[1]
                    elif result[1] > pm_osha:
                        pm_osha = result[1]

            result_pe = self.get_employee_handler(pe, 'pe', enum)
            if pe.find("N/A") == -1:
                pe_osha=""
            else:
                pe_osha="N/A"
            for result in result_pe:
                if result[0] == "OSHA Training":
                    if (pe_osha == "" or pe_osha == "N/A"):
                        pe_osha = result[1]
                    elif result[1] > pe_osha:
                        pe_osha = result[1]

            result_fm = self.get_employee_handler(fm, 'fm', enum)
            if pe.find("N/A") == -1:
                fm_osha=""
                fm_cpr=""
            else:
                fm_osha="N/A"
                fm_cpr="N/A"
            for result in result_fm:
                if result[0] == "OSHA Training":
                    if fm_osha == "" or fm_osha == "N/A":
                        fm_osha = result[1]
                    elif result[1] > fm_osha:
                        fm_osha = result[1]
                if result[0] == 'First Aid/CPR':
                    if fm_cpr == "" or fm_cpr == "N/A":
                        fm_cpr = result[1]
                    elif result[1] > fm_cpr:
                        fm_cpr = result[1]
            #endregion

            #region dev functions
            # posting_data.append({enum: {"Superintendent OSHA-30 Complete": osha30, "Superintendent First Aid/CPR Complete": cpr, "PM OSHA-10": pm_osha10, "PE OSHA-10": pe_osha10, "FM OSHA-10": fm_osha10, "FM First Aid/CPR":fm_cpr}})
            # posting_data.append([enum, sup, pm, pe, fm])
            #endregion
            posting_data.append({record.get('id'): [{"column_id": "2278907057596292","value": sup_osha}, {"column_id": "6782506684966788","value": sup_cpr}, {"column_id": "6985340810487684","value":pm_osha}, {"column_id": "1355841276274564","value":pe_osha}, {"column_id": "5859440903645060","value":fm_osha}, {"column_id": "8531968197453700","value":fm_cpr}]})
        return posting_data
    def post_data(self, posting_data):
        self.log.log(f"{self.timestamp()} posting...")
        self.row_data = []
        for i, dict in enumerate(posting_data):
            row_id = list(dict.keys())[0]
            new_row = smartsheet.models.Row()
            new_row.id = int(row_id)
            for column in dict.get(row_id):
                column_id = column.get("column_id")
                value = column.get("value")
                new_cell = smartsheet.models.Cell()
                new_cell.column_id = int(column_id)
                new_cell.value = value
                new_cell.strict = False
                new_row.cells.append(new_cell)
            self.row_data.append(new_row)
        resp = self.smart.Sheets.update_rows(
        int(self.safety_pl_sheet_id),      # sheet_id
        self.row_data)
        # self.log.log(f"row {int(i)+1}:  ", resp.message, "w/ row", row_id) 
        self.log.log(resp.message)
        self.log.log(f"{self.timestamp()} ~fin~")
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
        df_reordered = self.clean_smartsheet_data(df)
        data_dict = df_reordered.to_dict('records')
        self.log.log(f'''{self.timestamp()} Located {len(data_dict)} rows of Smartsheet data''') 
#   (approx. {round(int(len(data_dict))/80, 1)} minutes of buffering when gathering data)''')
        self.bamb_get_training()
        self.posting_data = self.gather_posting_data(data_dict)
        self.post_data(self.posting_data)
        self.post_update_stamp()

if __name__ == "__main__":
    tu = TrainingUpdater(smartsheet_token, bamb_api_key)
    tu.run()