import concurrent.futures
import pandas as pd
import xlrd
import os
import re
import sqlalchemy as sa
from itertools import repeat
from sqlalchemy import create_engine, inspect, text
from sqlalchemy import types
from sqlalchemy import exc


SERVER = 'zam-SPC-rey-db'
DATABASE = 'SPC-Rey'
conn_string = f'mssql+pyodbc://@{SERVER}/{DATABASE}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
sql_engine = create_engine(conn_string, fast_executemany=True)
sql_inspector = inspect(sql_engine)

pd.options.display.max_columns = None

DIRNAME = 'C:/Users/Z0205784/Downloads/'
EXT = 'xls'
COLUMNS_PRUEBAS = ["test", "serial_number", "evaluation", "sample_value", "test_result", "made_by", "equipment", 
                   'sample_plan', 'plan_result', 'plan_date', 'plan_hour', "lat_ticket"]
COLUMNS_TICKETS = ["lat_ticket", "lote", "line", "shift", "state", "ticket_result", "comments", "ticket_date",
                   "ticket_hour", "TRWNumber", "PartNumber", "LessFinish", "model", "serial_number"]


pruebas_dtypes = {
    "test": types.VARCHAR(),
    "serial_number": types.VARCHAR(length=50),
    "evaluation": types.VARCHAR(length=50),
    "sample_value": types.FLOAT,
    "test_result": types.VARCHAR(length=20),
    "made_by": types.BIGINT(),
    "equipment": types.VARCHAR(length=50),
    'sample_plan': types.VARCHAR(length=100),
    'plan_result': types.VARCHAR(length=20),
    'plan_date': types.DATE, 
    'plan_hour': types.TIME,
    "lat_ticket": types.VARCHAR(length=50),
    'usl': types.FLOAT(),
    'lsl': types.FLOAT()
}

ticket_dtypes = {
    "lat_ticket": types.VARCHAR(length=50),
    "lote": types.BIGINT(),
    "line": types.VARCHAR(length=50),
    "shift": types.SMALLINT(),
    "state": types.VARCHAR(length=50),
    "ticket_result": types.VARCHAR(length=20),
    "comments": types.VARCHAR(),
    "ticket_date": types.DATE(),
    "ticket_hour": types.TIME(),
    "TRWNumber": types.VARCHAR(length=50),
    "PartNumber": types.NVARCHAR(length=50),
    "LessFinish": types.VARCHAR(length=50),
    "model": types.VARCHAR(length=50),
    "serial_number": types.VARCHAR(length=50)
}

COL_ID_PRUEBAS = [3, 7, 9, 12, 15, 17, 20]  # [D, H, J, M, P, R, U]
LAT_LIST = [file for file in os.listdir(DIRNAME) if file.endswith(EXT)]
planes_df = pd.DataFrame()
pruebas_df = pd.DataFrame()
tickets_df = pd.DataFrame()

# Todo. Refactor this bitch
def extract_ticket_info(file_path, worksheet) -> pd.DataFrame:
    row_indices = [7, 8, 9, 10, 11, 12, 16]
    col_index = 4
    ticket_info_keys = [
        "Ticket",
        "Lote",
        "Linea",
        "Turno",
        "Estado",
        "Resultado",
        "Comentarios"
    ]

    ticket_info = {key: [worksheet.cell(row, col_index).value] for row, key in zip(row_indices, ticket_info_keys)}

    col_index = 10
    row_indices = [7, 8, 9, 10, 11, 12, 15]
    ticket_info_keys = ["Fecha de Creacion", "Hora de Creacion", "Numero de TRW", "Numero de Parte", "LessFinish",
                        "Modelo", "Numero de Serie"]

    ticket_info.update({key: [worksheet.cell(row, col_index).value] for row, key in zip(row_indices, ticket_info_keys)})
    ticket_info = pd.DataFrame().from_dict(ticket_info)
    # FIXME ... 
    ticket_info['Numero de Serie'] = ""

    return ticket_info

# TODO. Refactor this bitch
def read_excel_file(file_name: str) -> list:
    ticket_info_df = pd.DataFrame(columns=COLUMNS_TICKETS)
    file_path = f"{DIRNAME}{file_name}"
    workbook = xlrd.open_workbook(file_path, logfile=open(os.devnull, 'w'))
    worksheet = workbook.sheet_by_index(0)

    ticket_info_df = extract_ticket_info(file_path, worksheet)

    # Gets a list of each row that contains "Plan de pruebas" as cell value. 
    plan_rows = [i for i in range(21, worksheet.nrows) if worksheet.cell_value(i, 1) == 'Plan de Pruebas']
    # Gets the last row that has a 'prueba'
    last_prueba_row = max([i for i in range(0, worksheet.nrows) if worksheet.cell_value(i, 3) != ''])

    plan_list = []
    prueba_list = []
    ticket = ticket_info_df['Ticket'][0]
    for i in range(0, len(plan_rows)):
        plan = [worksheet.cell_value(plan_rows[i] + 1, col) for col in range(1, 20) if
                worksheet.cell_value(plan_rows[i] + 1, col) != ""]
        plan.append(ticket)
        # plan.append(ticket + " " + plan[0])
        plan_list.append(plan)

        row_start = plan_rows[i] + 3
        row_end = plan_rows[i + 1] if i + 1 < len(plan_rows) else last_prueba_row + 1
        for j in range(row_start, row_end):
            prueba = [worksheet.cell_value(j, col_id) for col_id in COL_ID_PRUEBAS]
            prueba.extend(plan)
            prueba_list.append(prueba)

    temp_pruebas_df = pd.DataFrame(prueba_list, columns=COLUMNS_PRUEBAS)
    return [temp_pruebas_df, ticket_info_df]


def get_spec_limits(evaluacion):
    lsl = None
    usl = None
    expression = r'[-+]?\d*\.\d+|\d+'

    values =  re.findall(expression, evaluacion)

    if 'Entre' in evaluacion and len(values) > 1:
        lsl = values[0]  
        usl = values[1] 
        return [lsl, usl]

    if 'Menor' in evaluacion:
        usl = float(re.findall(expression, evaluacion)[0])
        return [lsl, usl]

    if 'Mayor' in evaluacion:
        lsl = float(re.findall(expression, evaluacion)[0])
        return [lsl, usl]

    if 'Igual' in evaluacion:
        return [lsl, usl]

    return [lsl, usl]


count = 0
if __name__ == '__main__':
    pruebas_df = pd.DataFrame()
    planes_df = pd.DataFrame()
    tickets_df = pd.DataFrame()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(read_excel_file, LAT_LIST)
        for result in results:
            count += 1
            print(count)
            pruebas_df = pd.concat([pruebas_df, result[0]], ignore_index=True)
            tickets_df = pd.concat([tickets_df, result[1]], ignore_index=True)

    # * Cleaning lat_pruebas
    pruebas_df['sample_value'] = pruebas_df['sample_value'].astype(str).str.extract(r'(\d+\.\d+|\d+)').astype(float)
    USL = []
    LSL = []
    for index, row in pruebas_df.iterrows():
        lsl, usl = get_spec_limits(pruebas_df['evaluation'].iloc[index])
        LSL.append(lsl)
        USL.append(usl)

    pruebas_df['lsl'] = LSL
    pruebas_df['usl'] = USL
    pruebas_df['lsl'] = pd.to_numeric(pruebas_df['lsl'], errors='coerce')
    pruebas_df['usl'] = pd.to_numeric(pruebas_df['usl'], errors='coerce')   
    
    tickets_df.columns = COLUMNS_TICKETS
    pruebas_df['plan_date'] = pd.to_datetime(pruebas_df['plan_date'], errors='coerce')
    pruebas_df['plan_hour'] = pd.to_datetime(pruebas_df['plan_hour'], errors='coerce')
    tickets_df['ticket_date'] = pd.to_datetime(tickets_df['ticket_date'], errors='coerce')
    tickets_df['ticket_hour'] = pd.to_datetime(tickets_df['ticket_hour'], errors='coerce')
    
    print(pruebas_df.info())
    print(tickets_df.info())
    
    pruebas_df.dropna(subset=['sample_value'], inplace=True)
    output_path = r'C:\Repos\CSPM\LAT\Output Files'

    # Save into csv
    pruebas_df.to_csv(output_path + r'\\lat_pruebas.csv')
    tickets_df.to_csv(output_path + r'\\lat_tickets.csv')

    tickets_df.to_sql('lat_tickets', sql_engine, if_exists='append', index=False, dtype=ticket_dtypes)
    pruebas_df.to_sql('lat_samples', sql_engine, if_exists='append', index=False, dtype=pruebas_dtypes)
    
    print("Done")
