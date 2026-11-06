import re

import pandas as pd
from airflow.hooks.base import BaseHook
from coopdevsutils import executequery

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)


###############
#    VARS     #
###############

population = 49315949  # Dato extraído de manera manual del último censo del INE
current_year = 2025

###############
# FUNCTIONS   #
###############


# Get the sum of total values of simple numeric indicator
def get_agg(ind, df):
    return df.loc[df["indicator_code"] == ind, "value"].astype(float).sum()


# Get the sum of total values of an indicator type "table"
def get_table_total_agg(ind, df):
    print(f"[i] Executing function get_table_total_agg for indicator {ind}...")

    # Variables
    output = 0

    # Filter dataframe by indicator code
    filtered_value_arrays = df.loc[df["indicator_code"] == ind, "value"]

    # Iterate throuhg each array (table) of dataframe filtered_value_arrays
    for x in filtered_value_arrays:
        # Convert array to list
        values_list = x.split(",")

        # Get only last element which corresponds to "TOTAL ◻️ TOTAL" value, that is, the total sum of the table
        last_element = values_list[-1]

        if last_element != "NULL":
            # Aggragate value
            output = output + float(last_element)

    print(f"\t> Aggregated value of {ind}: {output}")

    return output


# Gets the raw data of str_suffix and value and returns a parsed dict
def parse_row(row):
    # Prepare format of "str_suffix" column cell content
    # stripped_row = row.str_suffix.lstrip("{").rstrip("}")
    stripped_row = row.str_suffix.lstrip(row.str_suffix[0]).rstrip(row.str_suffix[-1])
    stripped_row = re.sub(r'"', " ", stripped_row)
    str_suffix_list = stripped_row.split(",")

    # Prepare format of "value" column cell content
    # stripped_row = row.value.lstrip(row.value[0]).rstrip(row.value[-1])
    stripped_row = re.sub(r'"', " ", row.value)
    # stripped_row = re.sub(r'"', " ", stripped_row)
    value_list = stripped_row.split(",")

    # Create dictionary with keys as "str_suffix" column and values as "value" column
    return dict(zip(str_suffix_list, value_list))


# Get the sum of column total values of an indicator type "table"
def get_table_cell_agg(ind, row, column, df):
    print("**************************************************")
    print(
        f"[i] Executing function get_table_cell_agg for indicator {ind} row {row} and column {column}..."
    )

    # Variables
    cell_total = 0
    cell_tag = f"{column} ◻️ {row}"

    # Filter dataframe by indicator code
    df_filtered = df.loc[df["indicator_code"] == ind]

    # Save only the two required columns
    df_two_columns = df_filtered[["organization_name", "value", "str_suffix"]]

    # Iterate throuhg each row of dataframe df_two_columns
    for df_row in df_two_columns.itertuples(index=False):
        table_dict = parse_row(df_row)

        # Get all values of specified table cell
        for key in table_dict:
            # If the key matches the specified cell name
            if bool(re.search(cell_tag, key)) and table_dict[key] != "NULL":
                # print(
                #     f"{df_row.organization_name}: {cell_tag} and {key} -> {table_dict[key]}"
                # )
                # print(table_dict)

                # Aggragate value
                cell_total = cell_total + float(table_dict[key])

    print(f"\t> Aggregated value of {ind} [{row}][{column}]: {cell_total}")

    return cell_total


# Get the sum of column total values of an indicator type "table"
def get_table_column_agg(ind, column, df):
    print(f"\t> Calculating {ind} column [{column}] total...")
    return get_table_cell_agg(ind, "TOTAL", column, df)


# Get the sum of row total values of an indicator type "table"
def get_table_row_agg(ind, row, df):
    return get_table_cell_agg(ind, row, "TOTAL", df)


def add_row_to_dataframe(columns, rows):

    # Create new row as a dictionary to append to data record file
    row_dict = dict(zip(columns, rows))

    # Convert dictionary to dataframe
    return pd.DataFrame(row_dict)


#############
#    MAIN   #
#############

# Load data from db to dataframe
df_answers = pd.read_sql(
    "SELECT * FROM 'external'.answers_calc_agg WHERE campaign_name = 'CECJ 2026'  and  year  = '2026'",
    BaseHook.get_connection("DWH").get_uri(),
)

# Read CSV files
# df_answers = pd.read_csv(file_anwsers)
# if file is separated by ";", use this
# df_answers = pd.read_csv(file_anwsers, usecols=columns)

# Panda's options
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

# Clean cells
df_answers["value"] = df_answers["value"].str.replace("{", "")
df_answers["value"] = df_answers["value"].str.replace("}", "")

# Get DB connection to add new processed data
db_conn = BaseHook.get_connection("DWH").get_hook().get_sqlalchemy_engine()

###############
#    GRAPHS   #
###############

print("------[Initial calculations]-------")

# Calculated data:
# fairtrade_total = cecj-163_fairtrade_total + cecj-172_fairtrade_total - cecj-181_fairtrade_total
fairtrade_total = (
    get_table_column_agg("cecj-163", "cecj-fairtrade", df_answers)
    + get_table_column_agg("cecj-172", "cecj-fairtrade", df_answers)
    + get_table_column_agg("cecj-181", "cecj-fairtrade", df_answers)
)
print(f"fairtrade_total = {fairtrade_total}")

# ----------- #
#  Gráfica 1  # Evolución de las ventas de productos de Comercio Justo: este es el dato de facturación del Comercio Justo en el Estado Español
# ----------- #
# Fórmula: Total Extranjero + TOTAL CECJ (España) + FLO (€ corrientes)
# |__ Total Extranjero + TOTAL CECJ (España) = Total España + Extranjero (cecj-158)
# |__ FLO (€ corrientes) = Ventas totales FLO España (cecj-106_total) - Total productos FLO (cecj-163_fairtrade_total + cecj-172_fairtrade_total + cecj-181_fairtrade_total)
## Fórmula syh (agregados): cecj-158 + (cecj-106_total - (cecj-163_fairtrade_total + cecj-172_fairtrade_total + cecj-181_fairtrade_total))
#
# Indicadores               | Tipo entidad  | Ubicación en formulario SyH
# --------------------------|---------------------------------------------------------------------------------------------------------------
# cecj-158                  | Importadora   | Total España + Extranjero (Ventas > Canales)
# cecj-106_total            | Certificadora | Datos de venta de Productos de Comercio Justo con sello Fairtrade (Ventas > Datos de venta)
# cecj-163_fairtrade_total  | Importadora   | Productos de alimentación de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-172_fairtrade_total  | Importadora   | Productos de artesanía de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-181_fairtrade_total  | Importadora   | Otros productos de Comercio Justo por sello/certificación (Ventas > Productos)
#
# Tabla: comerciojusto.sales

print("-----------------------------")
print("---------[Graph 1]-----------")

cecj_g_1_data = [
    (
        get_agg("cecj-158", df_answers)
        + get_table_total_agg("cecj-106", df_answers)
        - fairtrade_total
    )
]

# Store in db
query = f"""
    INSERT INTO comerciojusto.sales 
    (year, type, origin, sales, amount) 
    VALUES 
    (
        {current_year}, 
        'FLO + CECJ',
        'España + Extranjero',
        'Total FLO + CECJ', 
        {float(cecj_g_1_data[0])}
    ) 
"""
executequery(query, db_conn)


# Print calculated value
print(cecj_g_1_data)
print("-----------------------------")

# ----------- #
#  Gráfica 2  # Evolución de las ventas de productos de Comercio Justo por parte de empresas licenciatarias del sello FT y de importadoras
# ----------- #
# En esta gráfica hay 2 líneas, y básicamente está desagregando los datos de Gráfica 1.
# |__ Línea superior (Ventas FT de empresas licenciatarias) - total_ventas_certificadora: cecj-106_total - cecj-163_fairtrade_total + cecj-172_fairtrade_total + cecj-181_fairtrade_total
# |__ Línea inferior (Ventas importadoras) - total_ventas_importadoras: cecj-158
#
# Indicadores               | Tipo entidad  | Ubicación en formulario SyH
# --------------------------|---------------------------------------------------------------------------------------------------------------
# cecj-158                  | Importadora   | Total España + Extranjero (Ventas > Canales)
# cecj-106_total            | Certificadora | Datos de venta de Productos de Comercio Justo con sello Fairtrade (Ventas > Datos de venta)
# cecj-163_fairtrade_total  | Importadora   | Productos de alimentación de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-172_fairtrade_total  | Importadora   | Productos de artesanía de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-181_fairtrade_total  | Importadora   | Otros productos de Comercio Justo por sello/certificación (Ventas > Productos)
#
# Tabla: comerciojusto.sales

print("-----------------------------")
print("---------[Graph 2]-----------")

cecj_g_2_data = [
    get_table_total_agg("cecj-106", df_answers) - fairtrade_total,
    get_agg("cecj-158", df_answers),
]

# Store in db
query = f"""
    INSERT INTO comerciojusto.sales 
    (year, type, origin, sales, amount) 
    VALUES 
    (
        {current_year}, 
        'CECJ',
        'España + Extranjero',
        'Total CECJ', 
        {float(cecj_g_2_data[1])}
    ) 
"""
executequery(query, db_conn)

query = f"""
    INSERT INTO comerciojusto.sales 
    (year, type, origin, sales, amount) 
    VALUES 
    (
        {current_year}, 
        'FLO',
        'España',
        'Total FLO (€ constantes)', 
        {float(cecj_g_2_data[0])}
    ) 
"""
executequery(query, db_conn)

# Print aggregated value
print(cecj_g_2_data)
print("-----------------------------")

# ----------- #
#  Gráfica 3  # Evolución del gasto anual medio por habitante en productos de Comercio Justo
# ----------- #
# Descripción: Hay que calcular la facturación en España y dividirlo por el número de habitantes.
# Fórmula: GASTO ANUAL MEDIO = ( (TOTAL CECJ (España) + FLO (€ constantes)) / Población España (miles) ) / 1000
# |__ TOTAL CECJ (España) + FLO (€ constantes) = cecj-155 + cecj-106_total - (cecj-163_fairtrade_total + cecj-172_fairtrade_total + cecj-181_fairtrade_total)
# |__ Población España (miles) = se coge el dato del INE. En el ejercicio de 2026 se cogen los datos de julio 2025, que serían 49.315.949. En miles esto serían 49.316.
#     https://www.ine.es/dyngs/Prensa/es/ECP2T24.htm#:~:text=La%20poblaci%C3%B3n%20de%20Espa%C3%B1a%20aument%C3%B3,1%20de%20julio%20de%202024
## Fórmula syh (agregados): ((cecj-155 + cecj-106_total - (cecj-163_fairtrade_total + cecj-172_fairtrade_total + cecj-181_fairtrade_total)) / 49.316 ) 1000
#
# Indicadores               | Tipo entidad  | Ubicación en formulario SyH
# --------------------------|---------------------------------------------------------------------------------------------------------------
# cecj-155                  | Importadora   | Total solo España (Ventas > Canales)
# cecj-106_total            | Certificadora | Datos de venta de Productos de Comercio Justo con sello Fairtrade (Ventas > Datos de venta)
# cecj-163_fairtrade_total  | Importadora   | Productos de alimentación de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-172_fairtrade_total  | Importadora   | Productos de artesanía de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-181_fairtrade_total  | Importadora   | Otros productos de Comercio Justo por sello/certificación (Ventas > Productos)
#
# Tabla: comerciojusto.average_expenditure

print("-----------------------------")
print("---------[Graph 3]-----------")

cecj_g_3_data = [
    (
        get_agg("cecj-155", df_answers)
        + get_table_total_agg("cecj-106", df_answers)
        - fairtrade_total
    )
    / (population / 1000)
]

query = f"""
    INSERT INTO comerciojusto.average_expenditure 
    (year, population, avg_exp_per_hab_ft_and_flo) 
    VALUES 
    (
        {current_year}, 
        {population / 1000},
        {float(cecj_g_3_data[0])}
    ) 
"""
executequery(query, db_conn)

# Print aggregated value
print(cecj_g_3_data)
print("-----------------------------")
