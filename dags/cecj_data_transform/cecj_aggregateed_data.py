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


# ----------- #
#  Gráfica 4  #  Ventas de Comercio Justo según grupos de producto
# ----------- #
# Descripción: En este caso se utilizan los datos de las importadoras y los datos de Fairtrade. Para construir la gráfica, tendremos 3 categorías.
# Categoría Alimentación: Se sumarán las ventas de productos con PVP estimado (IVA incluido) de todas las importadoras, los datos de venta de Fairtrade correspondientes a la categoría de alimentación, y se restarán las ventas de producto de las importadoras certificadas como Fairtrade.
# Categoría Artesanía: Total de horeca de los datos de Fairtrade + suma de todas las importadoras del total de hostelería + suma de todas las importadoras del total de vending  - (suma del total de productos de las importadoras con certificado FT – suma de todas las importadoras canales supermercados)
# Categoría Otros: corresponde a las categorías textil ropa, textil hogar y complementos
# Categoría Artesanía varias: corresponde a las categorías bisutería, juegos , decoración y otros
# Parece que la categoría otros y muebles (de OTROS)  no se han tenido en cuenta
#
#  Indicadores                        | Tipo entidad  | Ubicación en formulario SyH
# ------------------------------------|------------------------------------------------------------------------------------------------------------
# cecj-106                            | Certificadora | Total de los datos de Fairtrade
# cecj-159_[canal]_total              | Importadora   | Suma de todas las importadoras
# cecj-151_[canal]_total              | Importadora   | Suma de todas las importadoras del total de supermercados/hostelería/maquinas de vending...
# cecj-163_fairtrade_total            | Importadora   | Productos de alimentación de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-172_fairtrade_total            | Importadora   | Productos de artesanía de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-181_fairtrade_total            | Importadora   | Otros productos de Comercio Justo por sello/certificación (Ventas > Productos)
#
# Tabla: comerciojusto.products

print("-----------------------------")
print("---------[Graph 4]-----------")

cecj_g_4_labels = ["Alimentación", "Artesanía", "Otros"]

cecj_g_4_data = [
    (
        get_table_column_agg("cecj-159", "cecj-ventas-totales", df_answers)
        + (
            get_table_row_agg("cecj-106", "cecj-cafe", df_answers)
            + get_table_row_agg("cecj-106", "cecj-cacao-derivados", df_answers)
            + get_table_row_agg("cecj-106", "cecj-azucar-dulces", df_answers)
            + get_table_row_agg("cecj-106", "cecj-te-infusiones", df_answers)
            + get_table_row_agg("cecj-106", "cecj-otros-alimentacion", df_answers)
        )
        - get_table_column_agg("cecj-163", "cecj-fairtrade", df_answers)
    ),
    (
        get_table_column_agg("cecj-168", "cecj-ventas-totales", df_answers)
        + get_table_row_agg("cecj-106", "cecj-textil", df_answers)
        - get_table_column_agg("cecj-172", "cecj-fairtrade", df_answers)
    ),
    get_table_cell_agg("cecj-177", "cecj-otros", "cecj-ventas-totales", df_answers),
]

for i in range(len(cecj_g_4_data)):
    query = f"""
        INSERT INTO comerciojusto.products 
        (year, type, product, amount, category) 
        VALUES 
        (
            {current_year}, 
            "FLO + CECJ",
            "TOTAL",
            {float(cecj_g_4_data[i])},
            {cecj_g_4_labels[i]}
        ) 
    """
    executequery(query, db_conn)

# Print aggregated value
print(cecj_g_4_labels)
print(cecj_g_4_data)
print("-----------------------------")

# --------------------- #
#  Gráfica 5 (6 y 6.1)  #  Ventas de Comercio Justo según principales productos
# --------------------- #
# Descripción: En este caso es muy similar al de la Gráfica 4. pero en vez de agrupar por categorías más amplias, la categoría es de productos o varios productos. Para algunos de ellos usaremos los datos de Fairtrade y relativos a la certificación Fairtrade y para otros solo los datos de las importadoras (como en la Gráfica anterior).
# Categoría Cacao: corresponde a la categoría del cuestionario cacao y derivados. Suma de todas las importadoras cecj-159, total fila producto 1 + de Fairtrade cecj-106, suma de la fila producto  - suma de todas las importadoras cecj-163, fila producto de la columna 1 (Fairtrade)
# Categoría Café: corresponde a la categoría del cuestionario café. Suma de todas las importadoras cecj-159, total fila producto 1 + de Fairtrade cecj-106, suma de la fila producto  - suma de todas las importadoras cecj-163, fila producto de la columna 1 (Fairtrade)
# Categoría Azúcares&dulces: corresponde a las categorías del cuestionario azúcar, mermelada&miel y galletas. Suma de todas las importadoras cecj-159, total fila producto 1 + de Fairtrade cecj-106, suma de la fila producto  - suma de todas las importadoras cecj-163, fila producto de la columna 1 (Fairtrade)
# Categoría Té e infusiones: corresponde a la categoría del cuestionario té&infusiones. Suma de todas las importadoras cecj-159, total fila producto 1 + de Fairtrade cecj-106, suma de la fila producto  - suma de todas las importadoras cecj-163, fila producto de la columna 1 (Fairtrade)
# Categoría Otros alimentación: corresponde a las categorías del cuestionario bebidas alcohólicas, bebidas no alcohólicas, cereales (quinoa, muesli, arroz, pasta), otros. Suma de todas las importadoras cecj-159, total fila producto 1 + de Fairtrade cecj-106, suma de la fila producto  - suma de todas las importadoras cecj-163, fila producto de la columna 1 (Fairtrade)
# Categoría Textil: corresponde a las categorías del cuestionario textil ropa y textil hogar. Suma de todas las importadoras cecj-168, total fila producto 1 + de Fairtrade cecj-106, suma de la fila producto  - suma de todas las importadoras cecj-172, fila producto de la columna 1 (Fairtrade)
# Categoría Juguetes y otros: corresponde a las categorías del cuestionario complementos, bisutería, juegos y juguetes, decoración y otros (agrupados en la categoría de ARTESANÍA) y otros y muebles (agrupados en la categoría de OTROS). Suma de todas las importadoras cecj-168 y cecj-177, total fila producto
# Categoría Cosmética: corresponde a las categorías del cuestionario cosmética. Suma de todas las importadoras cecj-168 y cecj-177, total fila producto
# Las gráficas 6 y 6.1 se sacan a partir de estos datos
#
#  Indicadores                        | Tipo entidad  | Ubicación en formulario SyH
# ------------------------------------|------------------------------------------------------------------------------------------------------------
# cecj-106                            | Certificadora | Total de los datos de Fairtrade
# cecj-159_[canal]_total              | Importadora   | Suma de todas las importadoras
# cecj-163_fairtrade_total            | Importadora   | Productos de alimentación de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-168_fairtrade_total            | Importadora   | Productos de artesanía de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-172_fairtrade_total            | Importadora   | Productos de artesanía de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-177_fairtrade_total            | Importadora   | Otros productos de Comercio Justo
#
# Tabla: comerciojusto.products

print("-----------------------------")
print("---------[Graph 5]-----------")


cecj_g_5_labels = [
    "Cacao",
    "Café",
    "Azúcar y dulces",
    "Té e infusiones",
    "Otros Alimentación",
    "Textil",
    "Juguetes y otros",
    "Cosmética",
]

cecj_g_5_data = [
    (
        get_table_cell_agg(
            "cecj-159", "cecj-cacao-derivados", "cecj-ventas-totales", df_answers
        )
        + get_table_row_agg("cecj-106", "cecj-cacao", df_answers)
        - get_table_row_agg(
            "cecj-163", "cecj-cacao-derivados", "cecj-fairtrade", df_answers
        ),
    ),
    (
        get_table_cell_agg("cecj-159", "cecj-cafe", "cecj-ventas-totales", df_answers)
        + get_table_row_agg("cecj-106", "cecj-cafe", df_answers)
        - get_table_row_agg("cecj-163", "cecj-cafe", "cecj-fairtrade", df_answers),
    ),
    (
        get_table_cell_agg("cecj-159", "cecj-azucar", "cecj-ventas-totales", df_answers)
        + get_table_cell_agg(
            "cecj-159", "cecj-mermelada-miel", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-159", "cecj-galletas", "cecj-ventas-totales", df_answers
        )
        + get_table_row_agg("cecj-106", "cecj-azucar-dulces", df_answers)
        - (
            get_table_cell_agg(
                "cecj-163", "cecj-azucar", "cecj-ventas-totales", df_answers
            )
            + get_table_cell_agg(
                "cecj-163", "cecj-mermelada-miel", "cecj-ventas-totales", df_answers
            )
            + get_table_cell_agg(
                "cecj-163", "cecj-galletas", "cecj-ventas-totales", df_answers
            )
        ),
    ),
    (
        get_table_cell_agg(
            "cecj-159", "cecj-te-infusiones", "cecj-ventas-totales", df_answers
        )
        + get_table_row_agg("cecj-106", "cecj-te-infusiones", df_answers)
        - get_table_row_agg(
            "cecj-163", "cecj-te-infusiones", "cecj-fairtrade", df_answers
        ),
    ),
    (
        get_table_cell_agg(
            "cecj-159", "cecj-alcohol", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-159", "cecj-no-alcohol", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-159", "cecj-cereales", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-159", "cecj-otros", "cecj-ventas-totales", df_answers
        )
        + get_table_row_agg("cecj-106", "cecj-otros-alimentacion", df_answers)
        - (
            get_table_cell_agg(
                "cecj-163", "cecj-alcohol", "cecj-ventas-totales", df_answers
            )
            + get_table_cell_agg(
                "cecj-163", "cecj-no-alcohol", "cecj-ventas-totales", df_answers
            )
            + get_table_cell_agg(
                "cecj-163", "cecj-cereales", "cecj-ventas-totales", df_answers
            )
            + get_table_cell_agg(
                "cecj-163", "cecj-otros", "cecj-ventas-totales", df_answers
            )
        ),
    ),
    (
        get_table_cell_agg(
            "cecj-168", "cecj-textil-ropa", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-168", "cecj-textil-hogar", "cecj-ventas-totales", df_answers
        )
        + get_table_row_agg("cecj-106", "cecj-textil", df_answers)
        - (
            get_table_cell_agg(
                "cecj-172", "cecj-textil-ropa", "cecj-ventas-totales", df_answers
            )
            + get_table_cell_agg(
                "cecj-172", "cecj-textil-hogar", "cecj-ventas-totales", df_answers
            )
        ),
    ),
    (
        get_table_cell_agg(
            "cecj-168", "cecj-complementos", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-168", "cecj-bisuteria", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-168", "cecj-juegos-juguetes", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-168", "cecj-decoracion", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-168", "cecj-otros", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-177", "cecj-otros", "cecj-ventas-totales", df_answers
        )
        + get_table_cell_agg(
            "cecj-177", "cecj-muebles", "cecj-ventas-totales", df_answers
        )
    ),
    get_table_cell_agg("cecj-177", "cecj-cosmetica", "cecj-ventas-totales", df_answers),
]

for i in range(len(cecj_g_5_data)):
    query = f"""
        INSERT INTO comerciojusto.products 
        (year, type, product, amount, category) 
        VALUES 
        (
            {current_year}, 
            "FLO + CECJ",
            {cecj_g_5_labels[i]},
            {float(cecj_g_5_data[i])},
            {"Alimentación" if i <= 4 else ("Artesanía" if i <= 6 else "Otros")}
        ) 
    """
    executequery(query, db_conn)

# Print aggregated value
print(cecj_g_5_labels)
print(cecj_g_5_data)

print("-----------------------------")

# ----------------- #
#  Gráfica 7 (y 8)  #  Ventas de productos de Comercio Justo según canal de distribución (%)
# ----------------- #
# Descripción: En este caso hay varias categorías y cada categoría tiene un cálculo diferente.
# Categoría Supermercados: Total de retail de los datos de Fairtrade + suma de todas las importadoras del total de supermercados - (suma del total de productos de las importadoras con certificado FT – suma de todas las importadoras canales hostelería - suma de todas las importadoras canales vending)
# Categoría HORECA: Total de horeca de los datos de Fairtrade + suma de todas las importadoras del total de hostelería + suma de todas las importadoras del total de vending  - (suma del total de productos de las importadoras con certificado FT – suma de todas las importadoras canales supermercados)
# Categoría Tiendas de Comercio Justo: Suma de todas las importadoras del total de la fila Tiendas de Comercio Justo  de cecj-150 + suma de todas las importadoras del total de la fila Tiendas de Comercio Justo  de cecj-151
# Categoría Otros canales: Suma de todas las importadoras del total de de AAPP + suma de de todas las importadoras canales empresas + suma de de todas las importadoras canales distribuidoras
# Categoría Otros minoristas: Suma de de todas las importadoras canales Herboristerías, tiendas de productos ecológicos y/o dietéticos + suma de de todas las importadoras canales otros minoristas
#
#  Indicadores                             | Tipo entidad  | Ubicación en formulario SyH
# -----------------------------------------|------------------------------------------------------------------------------------------------------------
# cecj-106                                 | Certificadora | Total de retail/horeca de los datos de Fairtrade
# cecj-150_cecj-[canal]_total              | Importadora   | Suma de todas las importadoras del total de supermercados/hostelería/maquinas de vending...
# cecj-151_cecj-[canal]_total              | Importadora   | Suma de todas las importadoras del total de supermercados/hostelería/maquinas de vending...
# cecj-163_fairtrade_total                 | Importadora   | Productos de alimentación de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-172_fairtrade_total                 | Importadora   | Productos de artesanía de Comercio Justo por sello/certificación (Ventas > Productos)
# cecj-181_fairtrade_total                 | Importadora   | Otros productos de Comercio Justo por sello/certificación (Ventas > Productos)
#
# Gráfica 8:  Evolución de la distribución de las ventas por canales
# Descripción: Este gráfico añade los datos del gráfico anterior a uno de evolución.
#
# Tabla: comerciojusto.channels_flo

print("-----------------------------")
print("---------[Graph 7]-----------")

cecj_g_7_labels = [
    "Supermercados",
    "Hostelería, Restauración y Catering (HORECA)",
    "Tiendas de Comercio Justo",
    "Otros canales",
    "Otros minoristas",
]

# Calculated data:
# supermercados_total = cecj-150_cecj-supermercados_total + cecj-151_cecj-supermercados_total
supermercados_total = get_table_row_agg(
    "cecj-150", "cecj-supermercados", df_answers
) + get_table_row_agg("cecj-151", "cecj-supermercados", df_answers)

# Calculated data:
# hosteleria_total = cecj-150_cecj-hosteleria_total + cecj-151_cecj-hosteleria_total
hosteleria_total = get_table_row_agg(
    "cecj-150", "cecj-hosteleria", df_answers
) + get_table_row_agg("cecj-151", "cecj-hosteleria", df_answers)

# Calculated data:
# maquinas_vending_total = cecj-150_cecj-maquinas-vending_total + cecj-151_cecj-maquinas-vending_total
maquinas_vending_total = get_table_row_agg(
    "cecj-150", "cecj-maquinas-vending", df_answers
) + get_table_row_agg("cecj-151", "cecj-maquinas-vending", df_answers)

# Calculated data:
# tiendas_total = cecj-150_cecj-tiendas-organizaciones-cj_total + cecj-151_cecj-tiendas-organizaciones-cj_total
tiendas_total = get_table_row_agg(
    "cecj-150", "cecj-tiendas-organizaciones-cj", df_answers
) + get_table_row_agg("cecj-151", "cecj-tiendas-organizaciones-cj", df_answers)

# Calculated data:
# aapp_total = cecj-150_cecj-administraciones-publicas_total + cecj-151_cecj-administraciones-publicas_total
aapp_total = get_table_row_agg(
    "cecj-150", "cecj-administraciones-publicas", df_answers
) + get_table_row_agg("cecj-151", "cecj-administraciones-publicas", df_answers)

# Calculated data:
# empresas_total = cecj-150_cecj-empresas_total + cecj-151_cecj-empresas_total
empresas_total = get_table_row_agg(
    "cecj-150", "cecj-empresas", df_answers
) + get_table_row_agg("cecj-151", "cecj-empresas", df_answers)

# Calculated data:
# otras_distribuidoras_total = cecj-150_cecj-otras-distribuidoreas-importadoras_total + cecj-151_cecj-otras-distribuidoreas-importadoras_total
otras_distribuidoras_total = get_table_row_agg(
    "cecj-150", "cecj-otras-distribuidoreas-importadoras", df_answers
) + get_table_row_agg("cecj-151", "cecj-otras-distribuidoreas-importadoras", df_answers)

# Calculated data:
# herboristerias_total = cecj-150_cecj-herboristerias_total + cecj-151_cecj-herboristerias_total
herboristerias_total = get_table_row_agg(
    "cecj-150", "cecj-herboristerias", df_answers
) + get_table_row_agg("cecj-151", "cecj-herboristerias", df_answers)

# Calculated data:
# otros_minoristas_total = cecj-150_cecj-otros_minoristas_total + cecj-151_cecj-otros_minoristas_total
otros_minoristas_total = get_table_row_agg(
    "cecj-150", "cecj-otros_minoristas", df_answers
) + get_table_row_agg("cecj-151", "cecj-otros_minoristas", df_answers)


cecj_g_7_data = [
    (
        get_table_column_agg("cecj-106", "cecj-retail", df_answers)
        + supermercados_total
        - (fairtrade_total - hosteleria_total - maquinas_vending_total)
    ),
    (
        get_table_column_agg("cecj-106", "cecj-horeca", df_answers)
        + hosteleria_total
        + maquinas_vending_total
        - (fairtrade_total - supermercados_total)
    ),
    tiendas_total,
    (aapp_total + empresas_total + otras_distribuidoras_total),
    (herboristerias_total + otros_minoristas_total),
]

for i in range(len(cecj_g_7_data)):
    query = f"""
        INSERT INTO comerciojusto.channels_flo 
        (year, type, origin, channel, amount, aggregated) 
        VALUES 
        (
            {current_year}, 
            "CECJ",
            "España",
            {cecj_g_7_labels[i]},
            {float(cecj_g_7_data[i])},
            "0"
        ) 
    """
    executequery(query, db_conn)

# Print aggregated value
print(cecj_g_7_labels)
print(cecj_g_7_data)

print("-----------------------------")


# ----------- #
#  Gráfica 9  #  Ventas de productos de comercio justo de las importadoras CECJ según canal de distribución
# ----------- #
# Descripción: Este gráfico utiliza algunos datos parecidos, pero en todos los casos solo considera los datos de las importadoras (no se utilizan los datos de Fairtrade que sí utilizamos para el cálculo de las categorías de Supermercados y Horeca en el Gráfico 7.). Por lo tanto, el método de cálculo para cada categoría es igual.
#
#  Indicadores                 | Tipo entidad  | Ubicación en formulario SyH
# -----------------------------|---------------------------------------------------------------------------------------------------------------
# cecj-150_cecj-[canal]_total  | Importadora   | Suma de todas las importadoras del total de supermercados/hostelería/maquinas de vending...
# cecj-151_cecj-[canal]_total  | Importadora   | Suma de todas las importadoras del total de supermercados/hostelería/maquinas de vending...
#
# Tabla: comerciojusto.channels

print("-----------------------------")
print("---------[Graph 9]-----------")


cecj_g_9_labels = [
    "Tiendas y organizaciones de comercio justo",
    "Herboristerías, tiendas de productos ecológicos y/o dietéticos",
    "Otros minoristas (pequeño comercio, etc.)",
    "Administraciones públicas",
    "Empresas",
    "Supermercados y grandes superficies",
    "Hostelería (bares, restaurantes, catering…)",
    "Máquinas vending",
    "Otras distribuidoras/importadoras",
]

cecj_g_9_data = [
    tiendas_total,
    herboristerias_total,
    otros_minoristas_total,
    aapp_total,
    empresas_total,
    supermercados_total,
    hosteleria_total,
    maquinas_vending_total,
    otras_distribuidoras_total,
]

for i in range(len(cecj_g_9_data)):
    query = f"""
        INSERT INTO comerciojusto.channels
        (year, type, origin, channel, amount) 
        VALUES 
        (
            {current_year}, 
            "IMPORTADORAS",
            "ESPAÑA",
            {cecj_g_9_labels[i]},
            {float(cecj_g_9_data[i])}
        ) 
    """
    executequery(query, db_conn)

# Print aggregated value
print(cecj_g_9_labels)
print(cecj_g_9_data)

print("-----------------------------")
