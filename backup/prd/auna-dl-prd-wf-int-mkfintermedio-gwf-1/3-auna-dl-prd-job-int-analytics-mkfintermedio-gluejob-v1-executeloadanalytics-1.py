import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------ Reading data --------------------------------------------------------------------------
# Script generated for node S3 bucket
df_afiliaciones = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://auna-dlaprd-stage-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf_tablero_permanencia/"
        ],
        "recurse": True,
    },
)
# ------------------------------------------------------------ End Reading data --------------------------------------------------------------------------
# ------------------------------------------------------------ Mapping data ------------------------------------------------------------------------------
# Script generated for node ApplyMapping
map_df_afiliaciones = ApplyMapping.apply(
    frame=df_afiliaciones,
    mappings=[
        ("DES_CANAL", "string", "DES_CANAL", "string"),
        ("DES_OFICINA_VENTA", "string", "DES_OFICINA_VENTA", "string"),
        ("DES_GRUPO_VENDEDOR", "string", "DES_GRUPO_VENDEDOR", "string"),
        ("DES_SEDE", "string", "DES_SEDE", "string"),
        ("DES_PROGRAMA", "string", "DES_PROGRAMA", "string"),
        ("DES_RANGO_ETAREO", "string", "DES_RANGO_ETAREO", "string"),
        ("DES_SEGMENTO", "string", "DES_SEGMENTO", "string"),
        ("DES_TIPO_PAGO", "string", "DES_TIPO_PAGO", "string"),
        ("DES_COORDINADOR", "string", "DES_COORDINADOR", "string"),
        ("DES_ASESOR", "string", "DES_ASESOR", "string"),
        ("MES_ALTA", "string", "MES_ALTA", "string"),
        ("MES_BAJA", "string", "MES_BAJA", "string"),
        ("CAN_MES0", "int", "CAN_MES0", "int"),
        ("CAN_MES1", "int", "CAN_MES1", "int"),
        ("CAN_MES2", "int", "CAN_MES2", "int"),
        ("CAN_MES3", "int", "CAN_MES3", "int"),
        ("CAN_MES4", "int", "CAN_MES4", "int"),
        ("CAN_MES5", "int", "CAN_MES5", "int"),
        ("CAN_MES6", "int", "CAN_MES6", "int"),
        ("CAN_MES7", "int", "CAN_MES7", "int"),
        ("CAN_MES8", "int", "CAN_MES8", "int"),
        ("CAN_MES9", "int", "CAN_MES9", "int"),
        ("CAN_MES10", "int", "CAN_MES10", "int"),
        ("CAN_MES11", "int", "CAN_MES11", "int"),
        ("CAN_MES12", "int", "CAN_MES12", "int"),
        ("FEC_CARGA", "date", "FEC_CARGA", "date"),
    ],
)
# ------------------------------------------------------------ End Mapping data --------------------------------------------------------------------------
df_afil = map_df_afiliaciones.toDF()

#guardado
df_afil.write.mode('overwrite') \
		        .format('parquet') \
		        .save('s3://auna-dlaprd-analytics-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf_tablero_permanencia/')