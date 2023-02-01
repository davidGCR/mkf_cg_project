import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_stage_bucket', 's3_analytics_bucket', 'stage_cg_path', 'analytics_cg_path', 's3_tmp_cuotas_folder'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# set custom logging on
logger = glueContext.get_logger()

s3_stage_cuotas_gestionadas = f"s3://{args['s3_stage_bucket']}/{args['stage_cg_path']}"
s3_analytics_cuotas_gestionadas = f"s3://{args['s3_analytics_bucket']}/{args['analytics_cg_path']}"
#write into the log file with:
logger.info("================>job-name:" + args["JOB_NAME"]) 
logger.info(f"================>s3_stage_cuotas_gestionadas: {s3_stage_cuotas_gestionadas}")
logger.info(f"================>s3_analytics_cuotas_gestionadas: {s3_analytics_cuotas_gestionadas}")

print("Parameters")
print(f"s3_stage_bucket: {args['s3_stage_bucket']}")
print(f"s3_analytics_bucket: {args['s3_analytics_bucket']}")
print(f"stage_cg_path: {args['stage_cg_path']}")
print(f"analytics_cg_path: {args['analytics_cg_path']}")
print(f"s3_tmp_cuotas_folder: {args['s3_tmp_cuotas_folder']}")

#Copiar analytics a tmp
s3 = boto3.resource('s3')
src_bucket = s3.Bucket(args["s3_analytics_bucket"])
prefix = args["analytics_cg_path"]
tmp_key = f'structured-data/OLAP/pry-gestion-cobranza/{args["s3_tmp_cuotas_folder"]}/'
for obj in src_bucket.objects.filter(Prefix=prefix):
    logger.info(f"********* copying: {obj.key}----type: {type(obj)}")
    copy_source = {
        'Bucket': 'auna-dlaqa-analytics-s3',
        'Key': obj.key
    }
    s3.meta.client.copy(copy_source, args["s3_analytics_bucket"], tmp_key+obj.key.split('/')[-1])
    
# Data semanal
df_stg_cuotas = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            s3_stage_cuotas_gestionadas
        ]
    }
)

#Data historica
df_analytic_cuotas = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            # s3_analytics_cuotas_gestionadas
            f"s3://{args['s3_analytics_bucket']}/"+tmp_key
        ]
    }
)

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

logger.info(f"================>df_stg_cuotas rows: {df_stg_cuotas.count()}")
logger.info(f"================>df_analytics_cuotas rows: {df_analytics_cuotas.count()}")

# # analytics 2 cols
# df_join_cuotas = df_analytics_cuotas.select(F.col("COD_GF"),F.col("DES_PRD_GESTION"))
# logger.info(f"================>TWO COLUMNS df_join_cuotas rows: {df_join_cuotas.count()}, columns: {df_join_cuotas.columns}")

# # stage left analytics
# join_df_cuotas = df_stg_cuotas.join(df_join_cuotas, (df_stg_cuotas.COD_GRUPO_FAMILIAR == df_join_cuotas.COD_GF) & (df_stg_cuotas.DES_PERIODO_GESTION == df_join_cuotas.DES_PRD_GESTION),"left")
# logger.info(f"================>LEFT join_df_cuotas rows: {join_df_cuotas.count()}, columns: {join_df_cuotas.columns}")

# # Filter    
# join_df_cuotas = join_df_cuotas.filter(F.col("COD_GF").isNull())
# logger.info(f"================>FILTER join_df_cuotas rows: {join_df_cuotas.count()}, columns: {join_df_cuotas.columns}")


# join_df_cuotas = join_df_cuotas.select(F.col("COD_GRUPO_FAMILIAR"),F.col("TIP_GESTION"),F.col("DES_CANAL"),F.col("DES_OFICINA_VENTA"),F.col("NRO_AFILIADOS"),F.col("DES_ANTIGUEDAD_APORTANTE"),F.col("DES_UNI_NEGOCIO"),F.col("DES_PROGRAMA"),F.col("DES_SEGMENTO"),F.col("DES_TIPO_PAGO"),F.col("DES_FRECUENCIA_TARIFA"),F.col("NRO_MES_PRIMER_COBRO"),F.col("NRO_ANIO_PRIMER_COBRO"),F.col("DES_SEDE"),F.col("DES_SUPERVISOR"),F.col("DES_ASESOR"),F.col("MTO_PENDIENTE_INICIO_MES"),F.col("MTO_PENDIENTE_SEMANA1"),F.col("MTO_PENDIENTE_SEMANA2"),F.col("MTO_PENDIENTE_SEMANA3"),F.col("MTO_PENDIENTE_SEMANA4"),F.col("MTO_PENDIENTE_SEMANA5"),F.col("NRO_CUOTAS_PENDIENTES_INICIO_MES"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA1"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA2"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA3"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA4"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA5"),F.col("NRO_LLAMADAS_POR_EJECUTAR"),F.col("NRO_LLAMADAS_EJECUTADAS"),F.col("NRO_LLAMADAS_CONTESTADAS"),F.col("NRO_LLAMADAS_VALIDAS"),F.col("DES_TIPIFICACION_CASO"),F.col("DES_PERIODO_GESTION"),F.col("FEC_INICIO_SEMANA_GESTION"),F.col("FEC_FIN_SEMANA_GESTION"),F.col("DES_SEMANA"),F.col("FEC_CARGA"))
# logger.info(f"================>ADDED COLUMNS join_df_cuotas rows: {join_df_cuotas.count()}, columns: {join_df_cuotas.columns}")

# df_cuotas = df_analytics_cuotas.select(F.col("COD_GF"),F.col("TIP_GESTION"),F.col("DES_CANAL"),F.col("DES_OFICINA_VENTA"),F.col("NRO_AFILIADOS"),F.col("DES_ANTIGUEDAD_APORTANTE"),F.col("DES_UNI_NEGOCIO"),F.col("DES_PROGRAMA"),F.col("DES_SEGMENTO"),F.col("DES_TIPO_PAGO"),F.col("DES_FRECUENCIA_TARIFA"),F.col("NRO_MES_PRIMER_COBRO"),F.col("NRO_ANIO_PRIMER_COBRO"),F.col("DES_SEDE"),F.col("DES_SUPERVISOR"),F.col("DES_ASESOR"),F.col("MTO_PENDIENTE_INICIO_MES"),F.col("MTO_PENDIENTE_SEMANA1"),F.col("MTO_PENDIENTE_SEMANA2"),F.col("MTO_PENDIENTE_SEMANA3"),F.col("MTO_PENDIENTE_SEMANA4"),F.col("MTO_PENDIENTE_SEMANA5"),F.col("NRO_CUOTAS_PENDIENTES_INICIO_MES"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA1"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA2"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA3"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA4"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA5"),F.col("NRO_LLAMADAS_POR_EJECUTAR"),F.col("NRO_LLAMADAS_EJECUTADAS"),F.col("NRO_LLAMADAS_CONTESTADAS"),F.col("NRO_LLAMADAS_VALIDAS"),F.col("DES_TIPIFICACION_CASO"),F.col("DES_PRD_GESTION"),F.col("FEC_INICIO_SEMANA_GESTION").cast("date"),F.col("FEC_FIN_SEMANA_GESTION").cast("date"),F.col("DES_SEMANA"),F.col("FEC_CARGA"))
# logger.info(f"================>df_cuotas rows: {df_cuotas.count()}, columns: {df_cuotas.columns}")

# df_results = df_cuotas.union(join_df_cuotas)
# logger.info(f"================>UNION df_results rows: {df_results.count()}, columns: {df_results.columns}")
# logger.info(f"================>UNION df_results rows: {df_results.count()}, columns: {df_results.show(70)}")

client = boto3.client('s3')

#bucket='auna-dlaqa-raw-s3'

result= client.list_objects_v2(Bucket=args["s3_analytics_bucket"], Prefix = args["analytics_cg_path"])

if 'Contents' in result:
    #print("Key exists in the bucket.")
    df_join_cuotas = df_analytics_cuotas.select(F.col("COD_GF"),F.col("DES_PRD_GESTION"))
    
    join_df_cuotas = df_stg_cuotas.join(df_join_cuotas, (df_stg_cuotas.COD_GRUPO_FAMILIAR == df_join_cuotas.COD_GF) & (df_stg_cuotas.DES_PERIODO_GESTION == df_join_cuotas.DES_PRD_GESTION),"left")
    
    join_df_cuotas = join_df_cuotas.filter(F.col("COD_GF").isNull())
    
    join_df_cuotas = join_df_cuotas.select(F.col("COD_GRUPO_FAMILIAR"),F.col("TIP_GESTION"),F.col("DES_CANAL"),F.col("DES_OFICINA_VENTA"),F.col("NRO_AFILIADOS"),F.col("DES_ANTIGUEDAD_APORTANTE"),F.col("DES_UNI_NEGOCIO"),F.col("DES_PROGRAMA"),F.col("DES_SEGMENTO"),F.col("DES_TIPO_PAGO"),F.col("DES_FRECUENCIA_TARIFA"),F.col("NRO_MES_PRIMER_COBRO"),F.col("NRO_ANIO_PRIMER_COBRO"),F.col("DES_SEDE"),F.col("DES_SUPERVISOR"),F.col("DES_ASESOR"),F.col("MTO_PENDIENTE_INICIO_MES"),F.col("MTO_PENDIENTE_SEMANA1"),F.col("MTO_PENDIENTE_SEMANA2"),F.col("MTO_PENDIENTE_SEMANA3"),F.col("MTO_PENDIENTE_SEMANA4"),F.col("MTO_PENDIENTE_SEMANA5"),F.col("NRO_CUOTAS_PENDIENTES_INICIO_MES"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA1"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA2"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA3"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA4"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA5"),F.col("NRO_LLAMADAS_POR_EJECUTAR"),F.col("NRO_LLAMADAS_EJECUTADAS"),F.col("NRO_LLAMADAS_CONTESTADAS"),F.col("NRO_LLAMADAS_VALIDAS"),F.col("DES_TIPIFICACION_CASO"),F.col("DES_PERIODO_GESTION"),F.col("FEC_INICIO_SEMANA_GESTION").cast("date"),F.col("FEC_FIN_SEMANA_GESTION").cast("date"),F.col("DES_SEMANA"),F.col("FEC_CARGA"))
    
    df_cuotas = df_analytics_cuotas.select(F.col("COD_GF"),F.col("TIP_GESTION"),F.col("DES_CANAL"),F.col("DES_OFICINA_VENTA"),F.col("NRO_AFILIADOS"),F.col("DES_ANTIGUEDAD_APORTANTE"),F.col("DES_UNI_NEGOCIO"),F.col("DES_PROGRAMA"),F.col("DES_SEGMENTO"),F.col("DES_TIPO_PAGO"),F.col("DES_FRECUENCIA_TARIFA"),F.col("NRO_MES_PRIMER_COBRO"),F.col("NRO_ANIO_PRIMER_COBRO"),F.col("DES_SEDE"),F.col("DES_SUPERVISOR"),F.col("DES_ASESOR"),F.col("MTO_PENDIENTE_INICIO_MES"),F.col("MTO_PENDIENTE_SEMANA1"),F.col("MTO_PENDIENTE_SEMANA2"),F.col("MTO_PENDIENTE_SEMANA3"),F.col("MTO_PENDIENTE_SEMANA4"),F.col("MTO_PENDIENTE_SEMANA5"),F.col("NRO_CUOTAS_PENDIENTES_INICIO_MES"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA1"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA2"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA3"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA4"),F.col("NRO_CUOTAS_PENDIENTES_SEMANA5"),F.col("NRO_LLAMADAS_POR_EJECUTAR"),F.col("NRO_LLAMADAS_EJECUTADAS"),F.col("NRO_LLAMADAS_CONTESTADAS"),F.col("NRO_LLAMADAS_VALIDAS"),F.col("DES_TIPIFICACION_CASO"),F.col("DES_PRD_GESTION"),F.col("FEC_INICIO_SEMANA_GESTION").cast("date"),F.col("FEC_FIN_SEMANA_GESTION").cast("date"),F.col("DES_SEMANA"),F.col("FEC_CARGA"))
    
    df_results = df_cuotas.union(join_df_cuotas)
    logger.info(f"================>UNION df_results rows: {df_results.count()}, columns: {df_results.columns}")
    logger.info(f"================>UNION df_results type: {type(df_results)}")
    logger.info(f"================>UNION df_results data: {df_results.toDF().show()}")
    print('Union results: ', df_results.show())

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(args["s3_analytics_bucket"])
    bucket.objects.filter(Prefix=args["analytics_cg_path"]).delete()

    logger.info(f"================>Writing df_results in s3://{args['s3_analytics_bucket']}/{args['analytics_cg_path']}")
    df_results.write.mode('overwrite') \
    		        .format('parquet') \
    		        .save(f"s3://{args['s3_analytics_bucket']}/{args['analytics_cg_path']}")
else:
    logger.info(f"================>ELSEEEEEEE")
    logger.info(f"================>else  df_stg_cuotas rows: {df_stg_cuotas.count()} to save in: s3://{args['s3_analytics_bucket']}/{args['analytics_cg_path']}")
    df_stg_cuotas.write.mode('overwrite') \
    	        .format('parquet') \
    	        .save(f"s3://{args['s3_analytics_bucket']}/{args['analytics_cg_path']}")
    # df_stg_cuotas.write.mode("overwrite").parquet(f"s3://{args['s3_analytics_bucket']}/{args['analytics_cg_path']}")