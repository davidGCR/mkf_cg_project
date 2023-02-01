import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.sql import SparkSession, SQLContext
import pandas as pd 
import xlrd 
#import openpyxl 
from io import StringIO  
import boto3

#nuevos import
from datetime import date, timedelta, datetime
import pytz
import win32com.client
import os
import glob

args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_landing_bucket", "s3_mkf_path", "s3_sar_path"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()
bucket=args["s3_landing_bucket"]

#prd_today = datetime.now(pytz.timezone('America/Lima')).strftime(format='%d%m%Y')

#'MKF-ONCOSALUD-22112022'
prd_today = '30122022'

#today = date.today()
#anio_mes = (today.replace(day=1)).replace(day=1).strftime(format='%Y%m')
#anio_mes_ant = (today.replace(day=1) - timedelta(1)).replace(day=1).strftime(format='%Y%m')
#anio_mes_12_ant = (today.replace(day=1) - timedelta(360)).replace(day=1).strftime(format='%Y%m')

##################### Gral Functions #####################
def daterange(start_date, end_date):
    """Funcion para generar iterador de fechas en un rango

    Args:
        start_date (datetime): Rango de fecha minimo
        end_date (datetime): Rango de fecha maximo

    Yields:
        datetime: Fecha del dia
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def generate_week_filenames(folder_path, start_date, end_date, prefix='MKF-ONCOSALUD-'):
    filenames = []
    for single_date in daterange(start_date, end_date):
        data = prefix + single_date.strftime("%d%m%Y")
        filenames.append(data)
        
    logger.info(f"--------------->filenames: {filenames}")
    filepaths = [os.path.join(folder_path, fn) for fn in filenames]
    logger.info(f"--------------->filepaths: {filepaths}")
    return filenames, filepaths


def repair_corrupt_xls(filename, output_dir):
    """Convertir archivo corrupto xls a xlsx

    Args:
        filename (_type_): _description_
        output_dir (_type_): _description_
    """
    o = win32com.client.Dispatch("Excel.Application")
    o.Visible = False #name of file with extension
    file = os.path.basename(filename) 
    output = output_dir + '/' + file.replace('.xls','.xlsx')
    wb = o.Workbooks.Open(filename)
    wb.ActiveSheet.SaveAs(output,51)
    wb.Close(True)
    logger.info(f"============>Converted: {filename} to {output}")


def read_multi_xlsx_to_dataframe(folder_path, start_date, end_date, prefix='MKF-ONCOSALUD-'):
    """Leer archivos xls de una carpeta en S3 por semana y guardarlos en un dataframe.

    Args:
        folder_path (str): Path del folder en S3
        start_date (datetime): Rango de fecha minimo de la semana
        end_date (datetime): Rango de fecha maximo de la semana
        prefix (str, optional): Prefijo de nombre de archivos. Defaults to 'MKF-ONCOSALUD-'.

    Returns:
        pandas.Dataframe: Dataframe con la data de todos los XLS's
    """
    list_of_names = []
    for single_date in daterange(start_date, end_date):
        data = prefix + single_date.strftime("%d%m%Y")
        list_of_names.append(data)
        
    logger.info(f"--------------->files: {list_of_names}")

    df_consolidado = pd.DataFrame()
    for i in range(len(list_of_names)):
        df_tmp = pd.read_excel(folder_path + list_of_names[i]+".xls",sheet_name=list_of_names[i],engine='xlrd', usecols=['GrupoFamiliar', 'MotivoServicio'])
        df_consolidado = df_consolidado.append(df_tmp, ignore_index = True)

    logger.info(f"--------------->df_consolidado: {df_consolidado.shape}, columns: {df_consolidado.columns}")
    return df_consolidado




##################### Process MKF files #####################
dir = f's3://{args["s3_landing_bucket"]}/{args["s3_mkf_path"]}'
dir_1 = 'MKF-ONCOSALUD-'
extension = '.xls' 
directory_mkf = dir + dir_1 + prd_today + extension
# sheet_mkf = dir_1 + prd_today

# today = datetime.now(pytz.timezone('America/Lima'))
today = datetime.strptime('2023-01-14', '%Y-%m-%d')
start_date = (today - timedelta(6))
end_date = (today + timedelta(1))
logger.info(f"--------------->today: {today}, start: {start_date}, end: {end_date}")


# df_consolidado_sar = read_multi_xls(folder_path=dir, start_date=start_date, end_date=end_date, prefix="MKF-ONCOSALUD-")

filenames, filepaths = generate_week_filenames(folder_path=dir, start_date=start_date, end_date=end_date, prefix="MKF-ONCOSALUD-")

# list_of_mkf_names = []
# for single_date in daterange(start_date, end_date):
#     data = 'MKF-ONCOSALUD-' + single_date.strftime("%d%m%Y")
#     list_of_mkf_names.append(data)
    
# logger.info(f"--------------->mkf files: {list_of_mkf_names}")

# df_consolidado_mkf = pd.DataFrame()
# for i in range(len(list_of_mkf_names)):
#     df_sar = pd.read_excel(dir + list_of_mkf_names[i]+".xls",sheet_name=list_of_mkf_names[i],engine='xlrd', usecols=['GrupoFamiliar', 'MotivoServicio'])
#     df_consolidado_sar = df_consolidado_sar.append(df_sar, ignore_index = True)

# logger.info(f"--------------->df_consolidado_mkf: {df_consolidado_mkf.shape}, columns: {df_consolidado_mkf.columns}")





# s3_resource = boto3.resource('s3') 
# csv_buffer = StringIO()
# df_mkf = pd.read_excel(directory_mkf,sheet_name=sheet_mkf,engine='xlrd')
# df_mkf.to_csv(csv_buffer,index=False, sep='|', encoding='utf-8')
# s3_resource.Object(bucket,'structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/CSV/mkf/mkf' + prd_today + '.csv').put(Body=csv_buffer.getvalue())


##################### Process SAR files #####################
# dir1 = f's3://{args["s3_landing_bucket"]}/{args["s3_mkf_path"]}'
# dir_2 = 'SAR-ONCOSALUD-'
# directory_sar = dir1 + dir_2 + prd_today + extension
# sheet_sar = dir_2 + prd_today

# csv_buffer1 = StringIO()
# s3_resource1 = boto3.resource('s3') 

