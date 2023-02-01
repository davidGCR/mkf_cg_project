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

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket='auna-dlaqa-raw-s3'

#prd_today = datetime.now(pytz.timezone('America/Lima')).strftime(format='%d%m%Y')

#'MKF-ONCOSALUD-22112022'
prd_today = '30122022'

#today = date.today()
#anio_mes = (today.replace(day=1)).replace(day=1).strftime(format='%Y%m')
#anio_mes_ant = (today.replace(day=1) - timedelta(1)).replace(day=1).strftime(format='%Y%m')
#anio_mes_12_ant = (today.replace(day=1) - timedelta(360)).replace(day=1).strftime(format='%Y%m')

dir = 's3://auna-dlaqa-raw-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/xls_mkf/'
dir_1 = 'MKF-ONCOSALUD-'
extension = '.xls' 
directory_mkf = dir + dir_1 + prd_today + extension
sheet_mkf = dir_1 + prd_today

dir1 = 's3://auna-dlaqa-raw-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/xls_sar/'
dir_2 = 'SAR-ONCOSALUD-'
directory_sar = dir1 + dir_2 + prd_today + extension
sheet_sar = dir_2 + prd_today

csv_buffer = StringIO()
s3_resource = boto3.resource('s3') 

csv_buffer1 = StringIO()
s3_resource1 = boto3.resource('s3') 

#Lectura de archivos excel mkf y conversion a csv
df_mkf=pd.read_excel(directory_mkf,sheet_name=sheet_mkf,engine='xlrd')
df_mkf.to_csv(csv_buffer,index=False, sep='|', encoding='utf-8')
s3_resource.Object(bucket,'structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/CSV/mkf/mkf' + prd_today + '.csv').put(Body=csv_buffer.getvalue())

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

today = datetime.now(pytz.timezone('America/Lima'))
start_date = (today - timedelta(6))
end_date = (today + timedelta(1))

list_of_names = []
for single_date in daterange(start_date, end_date):
    data = 'SAR-ONCOSALUD-' + single_date.strftime("%d%m%Y")
    list_of_names.append(data)
    
print(list_of_names)
#list_of_names = ['SAR-ONCOSALUD-28122022','SAR-ONCOSALUD-29122022','SAR-ONCOSALUD-30122022']

df_consolidado_sar = pd.DataFrame()
## append datasets into the list
for i in range(len(list_of_names)):
    df_sar = pd.read_excel(dir1 + list_of_names[i]+".xls",sheet_name=list_of_names[i],engine='xlrd', usecols=['GrupoFamiliar', 'MotivoServicio'])
    df_consolidado_sar = df_consolidado_sar.append(df_sar, ignore_index = True)
#    df_sar.to_csv(csv_buffer1,index=False, sep='|', encoding='utf-8', header=None)
#    s3_resource1.Object(bucket,'structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/CSV/sar/sar' + prd_today + '.csv').put(Body=csv_buffer1.getvalue())

df_consolidado_sar.to_csv(csv_buffer1, encoding='utf-8', index=False)
s3_resource1.Object(bucket,'structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/CSV/sar/sar' + prd_today + '.csv').put(Body=csv_buffer1.getvalue())

# Script generated for node Data Catalog table
  = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": "|"},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            #"s3://auna-dlaqa-landingzone-s3/structured-data/olap/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf/"
            "s3://auna-dlaqa-raw-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/CSV/mkf/mkf" + prd_today + ".csv"
        ],
        "recurse": True,
    },
    transformation_ctx="DataCatalogtable_node1",
)

df_sar_mkf = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": "|"},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            #"s3://auna-dlaqa-landingzone-s3/structured-data/olap/dwh-gestionsalud/pry-gs-marketforceintermedio/sar/"
            "s3://auna-dlaqa-raw-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/CSV/sar/sar" + prd_today + ".csv"
        ],
        "recurse": True,
    },
    transformation_ctx="DataCatalogtable_node2",
)
#"quoteChar": '"', 

# Script generated for node m_mkf
m_mkf = ApplyMapping.apply(
    frame=df_mkf, mappings=[
        ("FechaGestion","string","FechaGestion","string"),
        ("TipoGestion","string","TipoGestion","string"),
        ("Periodo1erCobro","string","Periodo1erCobro","string"),
        ("Programa","string","Programa","string"),
        ("SegmentoComercial","string","SegmentoComercial","string"),
        ("TipoPago","string","TipoPago","string"),
        ("Frecuencia","string","Frecuencia","string"),
        ("TipoIdentificacion","string","TipoIdentificacion","string"),
        ("NumeroDocumento","string","NumeroDocumento","string"),
        ("GrupoFamiliar","string","GrupoFamiliar","string"),
        ("NumeroAfiliados","string","NumeroAfiliados","string"),
        ("NumeroCuotasPendientes","string","NumeroCuotasPendientes","string"),
        ("MontoPendiente","string","MontoPendiente","string"),
        ("LlamadasXEjecutar","string","LlamadasXEjecutar","string"),
        ("LlamadasEjecutadas","string","LlamadasEjecutadas","string"),
        ("LlamadasValidas","string","LlamadasValidas","string"),
        ("LlamadasContestadas","string","LlamadasContestadas","string"),
        ("MotivoRespuestaGestion","string","MotivoRespuestaGestion","string"),
        ], transformation_ctx="ApplyMapping_node3"
)

# Script generated for node m_mkf
m_sar = ApplyMapping.apply(
    frame=df_sar_mkf, mappings=[
        ("GrupoFamiliar","string","GrupoFamiliar","string"),
        ("MotivoServicio","string","MotivoServicio","string")
        ], transformation_ctx="ApplyMapping_node3"
)
#        ("NoTicket","string","NoTicket","string"),
#        ("FechaRegistro","string","FechaRegistro","string"),
#        ("MesRegistro","string","MesRegistro","string"),
#        ("HoraRegistro","string","HoraRegistro","string"),
#        ("AnioRegistro","string","AnioRegistro","string"),
#        ("SemanaAnio","string","SemanaAnio","string"),
#        ("NombreAfiliado","string","NombreAfiliado","string"),
#        ("TipoIdentificacion","string","TipoIdentificacion","string"),
#        ("NumeroDocumento","string","NumeroDocumento","string"),
#        ("CodigoAfiliado","string","CodigoAfiliado","string"),
#        ("GrupoFamiliar","string","GrupoFamiliar","string"),
#        ("Programa","string","Programa","string"),
#        ("Servicio","string","Servicio","string"),
#        ("MotivoServicio","string","MotivoServicio","string"),
#        ("TipoServicio","string","TipoServicio","string"),
#        ("DuracionServicio","string","DuracionServicio","string"),
#        ("UsuarioRegistro","string","UsuarioRegistro","string"),
#        ("NombreUsuarioRegistro","string","NombreUsuarioRegistro","string"),
#        ("DepartamentoUsuario","string","DepartamentoUsuario","string"),
#        ("DepartamentoOrigen","string","DepartamentoOrigen","string"),
#        ("UltFechaRegistro","string","UltFechaRegistro","string"),
#        ("UltMesRegistro","string","UltMesRegistro","string"),
#        ("UltHoraRegistro","string","UltHoraRegistro","string"),
#        ("UltAnioRegistro","string","UltAnioRegistro","string"),
#        ("UltSemanaAnio","string","UltSemanaAnio","string"),
#        ("UltDuracionPlanificada","string","UltDuracionPlanificada","string"),
#        ("UltDuracionEjecutada","string","UltDuracionEjecutada","string"),
#        ("UltUsuarioGestion","string","UltUsuarioGestion","string"),
#        ("UltIdUsuarioGestion","string","UltIdUsuarioGestion","string"),
#        ("UltDepartamento","string","UltDepartamento","string"),
#        ("EnFechaRegistro","string","EnFechaRegistro","string"),
#        ("EnMesRegistro","string","EnMesRegistro","string"),
#        ("EnHoraRegistro","string","EnHoraRegistro","string"),
#        ("EnAnioRegistro","string","EnAnioRegistro","string"),
#        ("EnSemanaAnio","string","EnSemanaAnio","string"),
#        ("EnDuracionPlanificada","string","EnDuracionPlanificada","string"),
#        ("EnDuracionEjecutada","string","EnDuracionEjecutada","string"),
#        ("EnUsuarioGestion","string","EnUsuarioGestion","string"),
#        ("EnIdUsuarioGestion","string","EnIdUsuarioGestion","string"),
#        ("EnDepartamento","string","EnDepartamento","string"),
#        ("EstadoServicio","string","EstadoServicio","string"),
#        ("FechaCorreo","string","FechaCorreo","string"),
#        ("TiempoEjecucionServicio","string","TiempoEjecucionServicio","string"),
#        ("ObservacionInicial","string","ObservacionInicial","string"),
#        ("ObservacionUltimaTarea","string","ObservacionUltimaTarea","string"),
#        ("EntidadReguladora","string","EntidadReguladora","string"),
#        ("IdEntidadSolicita","string","IdEntidadSolicita","string"),
#        ("EntidadSolicitaDescripcion","string","EntidadSolicitaDescripcion","string"),
#        ("IdEjecutor","string","IdEjecutor","string"),
#        ("IdResponsable","string","IdResponsable","string"),
#        ("IdServicio","string","IdServicio","string"),
#        ("IdCategoria","string","IdCategoria","string"),
#        ("IdSubCategoria","string","IdSubCategoria","string"),
#        ("IdPuntoOperacion","string","IdPuntoOperacion","string"),
#        ("IdDepartamentoResponsable","string","IdDepartamentoResponsable","string"),
#        ("IdDepartamentoEjecutor","string","IdDepartamentoEjecutor","string"),
#        ("EstadoRequerimientoCode","string","EstadoRequerimientoCode","string"),
#        ("UltTareaNombre","string","UltTareaNombre","string"),
#        ("EnTareaNombre","string","EnTareaNombre","string"),
#        ("Res_Consulta","string","Res_Consulta","string"),
#        ("Res_Reclamo","string","Res_Reclamo","string"),
#        ("Res_Requerimiento","string","Res_Requerimiento","string"),
#        ("Nom_Contacto","string","Nom_Contacto","string"),
#        ("Valida_Audio","string","Valida_Audio","string"),
#        ("Nvo_Servicio","string","Nvo_Servicio","string"),
#        ("Nvo_Motivo","string","Nvo_Motivo","string"),
#        ("ProgramaAunaSalud","string","ProgramaAunaSalud","string")

df_mkf = m_mkf.toDF()
df_sar = m_sar.toDF()

#select(F.col("CANAL_DIST").alias("DES_CANAL"),F.col("DES_OFICINA_VENTA"),F.col("GRUPO_VENDEDOR").alias("DES_GRUPO_VENDEDOR"),F.col("DES_SEDE"),F.col("PROGRAMA").alias("DES_PROGRAMA"),F.col("DES_RANGO_ETAREO"),F.col("DES_SEGMENTO"),F.col("DES_TIPO_PAGO"),F.col("DES_COORDINADOR"),F.col("DES_ASESOR"),F.col("MES_ALTA"),F.col("MES_BAJA"),F.col("C_CAN_MES0").cast('int').alias("CAN_MES0"),F.col("C_CAN_MES1").cast('int').alias("CAN_MES1"),F.col("C_CAN_MES2").cast('int').alias("CAN_MES2"),F.col("C_CAN_MES3").cast('int').alias("CAN_MES3"),F.col("C_CAN_MES4").cast('int').alias("CAN_MES4"),F.col("C_CAN_MES5").cast('int').alias("CAN_MES5"),F.col("C_CAN_MES6").cast('int').alias("CAN_MES6"),F.col("C_CAN_MES7").cast('int').alias("CAN_MES7"),F.col("C_CAN_MES8").cast('int').alias("CAN_MES8"),F.col("C_CAN_MES9").cast('int').alias("CAN_MES9"),F.col("C_CAN_MES10").cast('int').alias("CAN_MES10"),F.col("C_CAN_MES11").cast('int').alias("CAN_MES11"),F.col("C_CAN_MES12").cast('int').alias("CAN_MES12"),F.lit(F.current_date()).alias("FEC_CARGA"))

#print(df_sar.show(10))
#print(df_sar.count())
#df_sar.printSchema()

#guardado
df_mkf.write.mode('overwrite') \
		        .format('parquet') \
		        .save('s3://auna-dlaqa-raw-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf/')
		        
df_sar.write.mode('overwrite') \
		        .format('parquet') \
		        .save('s3://auna-dlaqa-raw-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/sar/')


s3 = boto3.resource('s3')
bucket = s3.Bucket('auna-dlaqa-raw-s3')
bucket.objects.filter(Prefix="structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/CSV/").delete()