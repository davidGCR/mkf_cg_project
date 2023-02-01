import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import boto3
import botocore

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

#crear un tmp, donde se copia desde analytics a una carpeta temporal
#s3_resource = boto3.resource('s3')

bkt = 'auna-dlaqa-analytics-s3'

#s3://auna-dlaqa-analytics-s3/structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/
#new_bucket_name = "s3://auna-dlaqa-analytics-s3/structured-data/OLAP/pry-gestion-cobranza/tmp_cuotas/"
#bucket_to_copy = "s3://auna-dlaqa-analytics-s3/structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/"


#for key in s3_resource.list_objects(Bucket=bucket_to_copy)['Contents']:
#    files = key['Key']
#    copy_source = {'Bucket': "bucket_to_copy",'Key': files}
#    s3_resource.meta.client.copy(copy_source, new_bucket_name, files)
#    print(files)

#este se usa
s3_copy = boto3.resource('s3')
copy_source = {'Bucket': 'auna-dlaqa-analytics-s3','Key': 'structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/'}
bucket = s3_copy.Bucket('auna-dlaqa-analytics-s3')
bucket.copy(copy_source, 'structured-data/OLAP/pry-gestion-cobranza/tmp_cuotas/')


#eliminar los parquet de la carpeta analytics

#usar el tmp en la logica
#guardar la data en la carpeta analytics


# ------------------------------------------------------------ Reading data --------------------------------------------------------------------------
# Script generated for node S3 bucket
df_stg_cuotas = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://auna-dlaqa-stage-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf_cuotas_gestionadas/"
        ]
    }
)

# Script generated for node S3 bucket
df_analytic_cuotas = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://auna-dlaqa-analytics-s3/structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/"
            #"s3://auna-dlaqa-analytics-s3/structured-data/OLAP/pry-gestion-cobranza/tmp_cuotas/"
        ]
    }
)

# ------------------------------------------------------------ End Reading data --------------------------------------------------------------------------
# ------------------------------------------------------------ Mapping data ------------------------------------------------------------------------------
# Script generated for node ApplyMapping
map_stg_cuotas = ApplyMapping.apply(
    frame=df_stg_cuotas,
    mappings=[
        ("COD_GRUPO_FAMILIAR", "string", "COD_GRUPO_FAMILIAR", "string"),
        ("TIP_GESTION", "string", "TIP_GESTION", "string"),
        ("DES_CANAL", "string", "DES_CANAL", "string"),
        ("DES_OFICINA_VENTA", "string", "DES_OFICINA_VENTA", "string"),
        ("DES_GRUPO_VENDEDOR", "string", "DES_GRUPO_VENDEDOR", "string"),
        ("NRO_AFILIADOS", "int", "NRO_AFILIADOS", "int"),
        ("DES_ANTIGUEDAD_APORTANTE", "string", "DES_ANTIGUEDAD_APORTANTE", "string"),
        ("DES_UNI_NEGOCIO", "string", "DES_UNI_NEGOCIO", "string"),
        ("DES_PROGRAMA", "string", "DES_PROGRAMA", "string"),
        ("DES_SEGMENTO", "string", "DES_SEGMENTO", "string"),
        ("DES_TIPO_PAGO", "string", "DES_TIPO_PAGO", "string"),
        ("DES_FRECUENCIA_TARIFA", "string", "DES_FRECUENCIA_TARIFA", "string"),
        ("NRO_MES_PRIMER_COBRO", "int", "NRO_MES_PRIMER_COBRO", "int"),
        ("NRO_ANIO_PRIMER_COBRO", "int", "NRO_ANIO_PRIMER_COBRO", "int"),
        ("DES_SEDE", "string", "DES_SEDE", "string"),
        ("DES_SUPERVISOR", "string", "DES_SUPERVISOR", "string"),
        ("DES_ASESOR", "string", "DES_ASESOR", "string"),
        ("MTO_PENDIENTE_INICIO_MES", "decimal", "MTO_PENDIENTE_INICIO_MES", "decimal"),
        ("MTO_PENDIENTE_SEMANA1", "decimal", "MTO_PENDIENTE_SEMANA1", "decimal"),
        ("MTO_PENDIENTE_SEMANA2", "decimal", "MTO_PENDIENTE_SEMANA2", "decimal"),
        ("MTO_PENDIENTE_SEMANA3", "decimal", "MTO_PENDIENTE_SEMANA3", "decimal"),
        ("MTO_PENDIENTE_SEMANA4", "decimal", "MTO_PENDIENTE_SEMANA4", "decimal"),
        ("MTO_PENDIENTE_SEMANA5", "decimal", "MTO_PENDIENTE_SEMANA5", "decimal"),
        ("NRO_CUOTAS_PENDIENTES_INICIO_MES", "int", "NRO_CUOTAS_PENDIENTES_INICIO_MES", "int"),
        ("NRO_CUOTAS_PENDIENTES_SEMANA1", "int", "NRO_CUOTAS_PENDIENTES_SEMANA1", "int"),
        ("NRO_CUOTAS_PENDIENTES_SEMANA2", "int", "NRO_CUOTAS_PENDIENTES_SEMANA2", "int"),
        ("NRO_CUOTAS_PENDIENTES_SEMANA3", "int", "NRO_CUOTAS_PENDIENTES_SEMANA3", "int"),
        ("NRO_CUOTAS_PENDIENTES_SEMANA4", "int", "NRO_CUOTAS_PENDIENTES_SEMANA4", "int"),
        ("NRO_CUOTAS_PENDIENTES_SEMANA5", "int", "NRO_CUOTAS_PENDIENTES_SEMANA5", "int"),
        ("NRO_LLAMADAS_POR_EJECUTAR", "int", "NRO_LLAMADAS_POR_EJECUTAR", "int"),
        ("NRO_LLAMADAS_EJECUTADAS", "int", "NRO_LLAMADAS_EJECUTADAS", "int"),
        ("NRO_LLAMADAS_CONTESTADAS", "int", "NRO_LLAMADAS_CONTESTADAS", "int"),
        ("NRO_LLAMADAS_VALIDAS", "int", "NRO_LLAMADAS_VALIDAS", "int"),
        ("DES_TIPIFICACION_CASO", "string", "DES_TIPIFICACION_CASO", "string"),
        ("DES_PERIODO_GESTION", "string", "DES_PERIODO_GESTION", "string"),
        ("FEC_INICIO_SEMANA_GESTION", "date", "FEC_INICIO_SEMANA_GESTION", "date"),
        ("FEC_FIN_SEMANA_GESTION", "date", "FEC_FIN_SEMANA_GESTION", "date"),
        ("DES_SEMANA", "string", "DES_SEMANA", "string"),
        ("FEC_CARGA", "date", "FEC_CARGA", "date"),
    ],
)

map_analytics_cuotas = ApplyMapping.apply(
    frame=df_analytic_cuotas,
    mappings=[
        ("COD_GRUPO_FAMILIAR", "string", "COD_GF", "string"),
        ("TIP_GESTION", "string", "TIP_GESTION", "string"),
        ("DES_CANAL", "string", "DES_CANAL", "string"),
        ("DES_OFICINA_VENTA", "string", "DES_OFICINA_VENTA", "string"),
        ("DES_GRUPO_VENDEDOR", "string", "DES_GRUPO_VENDEDOR", "string"),
        ("NRO_AFILIADOS", "int", "NRO_AFILIADOS", "int"),
        ("DES_ANTIGUEDAD_APORTANTE", "string", "DES_ANTIGUEDAD_APORTANTE", "string"),
        ("DES_UNI_NEGOCIO", "string", "DES_UNI_NEGOCIO", "string"),
        ("DES_PROGRAMA", "string", "DES_PROGRAMA", "string"),
        ("DES_SEGMENTO", "string", "DES_SEGMENTO", "string"),
        ("DES_TIPO_PAGO", "string", "DES_TIPO_PAGO", "string"),
        ("DES_FRECUENCIA_TARIFA", "string", "DES_FRECUENCIA_TARIFA", "string"),
        ("NRO_MES_PRIMER_COBRO", "int", "NRO_MES_PRIMER_COBRO", "int"),
        ("NRO_ANIO_PRIMER_COBRO", "int", "NRO_ANIO_PRIMER_COBRO", "int"),
        ("DES_SEDE", "string", "DES_SEDE", "string"),
        ("DES_SUPERVISOR", "string", "DES_SUPERVISOR", "string"),
        ("DES_ASESOR", "string", "DES_ASESOR", "string"),
        ("MTO_PENDIENTE_INICIO_MES", "decimal", "MTO_PENDIENTE_INICIO_MES", "decimal"),
        ("MTO_PENDIENTE_SEMANA1", "decimal", "MTO_PENDIENTE_SEMANA1", "decimal"),
        ("MTO_PENDIENTE_SEMANA2", "decimal", "MTO_PENDIENTE_SEMANA2", "decimal"),
        ("MTO_PENDIENTE_SEMANA3", "decimal", "MTO_PENDIENTE_SEMANA3", "decimal"),
        ("MTO_PENDIENTE_SEMANA4", "decimal", "MTO_PENDIENTE_SEMANA4", "decimal"),
        ("MTO_PENDIENTE_SEMANA5", "decimal", "MTO_PENDIENTE_SEMANA5", "decimal"),
        ("NRO_CUOTAS_PENDIENTES_INICIO_MES", "int", "NRO_CUOTAS_PENDIENTES_INICIO_MES", "int"),
        ("NRO_CUOTAS_PENDIENTES_SEMANA1", "int", "NRO_CUOTAS_PENDIENTES_SEMANA1", "int"),
        ("NRO_CUOTAS_PENDIENTES_SEMANA2", "int", "NRO_CUOTAS_PENDIENTES_SEMANA2", "int"),
        ("NRO_CUOTAS_PENDIENTES_SEMANA3", "int", "NRO_CUOTAS_PENDIENTES_SEMANA3", "int"),
        ("NRO_CUOTAS_PENDIENTES_SEMANA4", "int", "NRO_CUOTAS_PENDIENTES_SEMANA4", "int"),
        ("NRO_CUOTAS_PENDIENTES_SEMANA5", "int", "NRO_CUOTAS_PENDIENTES_SEMANA5", "int"),
        ("NRO_LLAMADAS_POR_EJECUTAR", "int", "NRO_LLAMADAS_POR_EJECUTAR", "int"),
        ("NRO_LLAMADAS_EJECUTADAS", "int", "NRO_LLAMADAS_EJECUTADAS", "int"),
        ("NRO_LLAMADAS_CONTESTADAS", "int", "NRO_LLAMADAS_CONTESTADAS", "int"),
        ("NRO_LLAMADAS_VALIDAS", "int", "NRO_LLAMADAS_VALIDAS", "int"),
        ("DES_TIPIFICACION_CASO", "string", "DES_TIPIFICACION_CASO", "string"),
        ("DES_PERIODO_GESTION", "string", "DES_PRD_GESTION", "string"),
        ("FEC_INICIO_SEMANA_GESTION", "date", "FEC_INICIO_SEMANA_GESTION", "date"),
        ("FEC_FIN_SEMANA_GESTION", "date", "FEC_FIN_SEMANA_GESTION", "date"),
        ("DES_SEMANA", "string", "DES_SEMANA", "string"),
        ("FEC_CARGA", "date", "FEC_CARGA", "date"),
    ],
)
# ------------------------------------------------------------ End Mapping data --------------------------------------------------------------------------
df_stg_cuotas = map_stg_cuotas.toDF()
df_analytics_cuotas = map_analytics_cuotas.toDF()

client = boto3.client('s3')

#bucket='auna-dlaqa-raw-s3'

result= client.list_objects_v2(Bucket=bkt, Prefix = 'structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/')

if 'Contents' in result:
    #print("Key exists in the bucket.")
    df_join_cuotas = df_analytics_cuotas.select(F.col("COD_GF"),F.col("DES_PRD_GESTION"))
    
    join_df_cuotas = df_stg_cuotas.join(df_join_cuotas, (df_stg_cuotas.COD_GRUPO_FAMILIAR == df_join_cuotas.COD_GF) & (df_stg_cuotas.DES_PERIODO_GESTION == df_join_cuotas.DES_PRD_GESTION),"left")
    
    join_df_cuotas = join_df_cuotas.filter(F.col("COD_GF").isNull())
    
    join_df_cuotas = join_df_cuotas.select(F.col("COD_GRUPO_FAMILIAR"),F.col("TIP_GESTION"),F.col("DES_CANAL"),F.col("DES_OFICINA_VENTA"),F.col("NRO_AFILIADOS"),F.col("DES_ANTIGUEDAD_APORTANTE"),F.col("DES_UNI_NEGOCIO"),F.col("DES_PROGRAMA"),F.col("DES_SEGMENTO"),F.col("DES_TIPO_PAGO"),F.col("DES_FRECUENCIA_TARIFA"),F.col("NRO_MES_PRIMER_COBRO"),F.col("NRO_ANIO_PRIMER_COBRO"),F.col("DES_SEDE"),F.col("DES_SUPERVISOR"),F.col("DES_ASESOR"),F.col("MTO_PENDIENTE_INICIO_MES"),F.col("MTO_PENDIENTE_SEMANA1"),F.col("MTO_PENDIENTE_SEMANA2"),F.col("MTO_PENDIENTE_SEMANA3"),F.col("MTO_PENDIENTE_SEMANA4"),F.col("MTO_PENDIENTE_SEMANA5"),F.col("NRO_CUOTAS_PENDIENTES_INICIO_MES"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA1"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA2"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA3"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA4"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA5"),F.col("NRO_LLAMADAS_POR_EJECUTAR"),F.col("NRO_LLAMADAS_EJECUTADAS"),F.col("NRO_LLAMADAS_CONTESTADAS"),F.col("NRO_LLAMADAS_VALIDAS"),F.col("DES_TIPIFICACION_CASO"),F.col("DES_PERIODO_GESTION"),F.col("FEC_INICIO_SEMANA_GESTION").cast("date"),F.col("FEC_FIN_SEMANA_GESTION").cast("date"),F.col("DES_SEMANA"),F.col("FEC_CARGA"))
    
    df_cuotas = df_analytics_cuotas.select(F.col("COD_GF"),F.col("TIP_GESTION"),F.col("DES_CANAL"),F.col("DES_OFICINA_VENTA"),F.col("NRO_AFILIADOS"),F.col("DES_ANTIGUEDAD_APORTANTE"),F.col("DES_UNI_NEGOCIO"),F.col("DES_PROGRAMA"),F.col("DES_SEGMENTO"),F.col("DES_TIPO_PAGO"),F.col("DES_FRECUENCIA_TARIFA"),F.col("NRO_MES_PRIMER_COBRO"),F.col("NRO_ANIO_PRIMER_COBRO"),F.col("DES_SEDE"),F.col("DES_SUPERVISOR"),F.col("DES_ASESOR"),F.col("MTO_PENDIENTE_INICIO_MES"),F.col("MTO_PENDIENTE_SEMANA1"),F.col("MTO_PENDIENTE_SEMANA2"),F.col("MTO_PENDIENTE_SEMANA3"),F.col("MTO_PENDIENTE_SEMANA4"),F.col("MTO_PENDIENTE_SEMANA5"),F.col("NRO_CUOTAS_PENDIENTES_INICIO_MES"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA1"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA2"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA3"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA4"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA5"),F.col("NRO_LLAMADAS_POR_EJECUTAR"),F.col("NRO_LLAMADAS_EJECUTADAS"),F.col("NRO_LLAMADAS_CONTESTADAS"),F.col("NRO_LLAMADAS_VALIDAS"),F.col("DES_TIPIFICACION_CASO"),F.col("DES_PRD_GESTION"),F.col("FEC_INICIO_SEMANA_GESTION").cast("date"),F.col("FEC_FIN_SEMANA_GESTION").cast("date"),F.col("DES_SEMANA"),F.col("FEC_CARGA"))
    
    df_results = df_cuotas.union(join_df_cuotas)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('auna-dlaqa-analytics-s3')
    bucket.objects.filter(Prefix="structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/").delete()
    
    df_results.write.mode('overwrite') \
    		        .format('parquet') \
    		        .save('s3://auna-dlaqa-analytics-s3/structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/')
    		        
    #print(df_results.show(70))
    #df_results.printSchema()
    
else:
    #print("Key doesn't exist in the bucket.")
    #s3 = boto3.resource('s3')
    #bucket = s3.Bucket('auna-dlaqa-analytics-s3')
    #bucket.objects.filter(Prefix="structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/").delete()
    
    df_stg_cuotas.write.mode('overwrite') \
    	        .format('parquet') \
    	        .save('s3://auna-dlaqa-analytics-s3/structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/')

#guardado
#df_results.write.mode('overwrite') \
#		        .format('parquet') \
#		        .save('s3://auna-dlaqa-analytics-s3/structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/')