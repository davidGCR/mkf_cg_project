import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

#nuevos import
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import to_date,date_format,add_months,upper,lpad

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ------------------------------------------------------------ Reading data --------------------------------------------------------------------------
# Script generated for afiliaciones
df_afiliaciones = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://auna-dlaqa-analytics-s3/structured-data/OLAP/dwh-Modelos-Operacional-Poblaciones/pry-mop-Poblacion-Oncosalud/poblacion-afiliaciones/"
        ],
        "recurse": True,
    },
    #transformation_ctx="S3bucket_node1",
)

# Script generated for mkf
df_mkf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://auna-dlaqa-raw-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf/"
        ],
        "recurse": True,
    },
    #transformation_ctx="S3bucket_node1",
)

# Script generated for sar
df_sar_mkf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://auna-dlaqa-raw-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/sar/"
        ],
        "recurse": True,
    },
    #transformation_ctx="S3bucket_node1",
)

# ------------------------------------------------------------ End Reading data --------------------------------------------------------------------------
# ------------------------------------------------------------ Mapping data ------------------------------------------------------------------------------
# Script generated for node ApplyMapping
map_df_afiliaciones = ApplyMapping.apply(
    frame=df_afiliaciones,
    mappings=[
        ("NUM_POS","string", "NUM_POS", "string"),
        ("COD_GRUPO_FAMILIAR", "string", "COD_GRUPO_FAMILIAR", "string"),
        ("PROGRAMA", "string", "PROGRAMA", "string"),
        ("DES_OFICINA_VENTA", "string", "DES_OFICINA_VENTA", "string"),
        ("CANAL_DIST", "string", "CANAL_DIST", "string"),
        ("COD_FREC_PAGO", "string", "COD_FREC_PAGO", "string"),
        ("COD_TIP_PAGO", "string", "COD_TIP_PAGO", "string"),
        ("ESTADO_AFILIADO", "string", "ESTADO_AFILIADO", "string"),
        ("FEC_PRIMER_COBRO", "date", "FEC_PRIMER_COBRO", "date"),
        ("SEDE_COMERCIAL", "string", "SEDE_COMERCIAL", "string"),
        ("GRUPO_VENDEDOR", "string", "GRUPO_VENDEDOR", "string"),
        ("NOMBRE_VENDEDOR", "string", "NOMBRE_VENDEDOR", "string"),
        ("segmento_comercial", "string", "segmento_comercial", "string"),
        ("CODIGO_ESTADO_AFILIADO", "string", "CODIGO_ESTADO_AFILIADO", "string"),
        ("TIP_AFIL", "string", "TIP_AFIL", "string"),
        ("des_producto","string","des_producto","string"),
        ("frecuencia_pago","string","frecuencia_pago","string"),
        ("FEC_AFIL_CONVERSION", "date", "FEC_AFIL_CONVERSION", "date"),
        ("FEC_AFIL_DESDE", "date", "FEC_AFIL_DESDE", "date")
    ],
)
#("RANGO_EDAD_CONTRATANTE", "string", "RANGO_EDAD_CONTRATANTE", "string"),
#("FEC_EJEC_BAJA_AFIL", "date", "FEC_EJEC_BAJA_AFIL", "date"),

# Script generated for node m_mkf
map_mkf = ApplyMapping.apply(
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
        ("MotivoRespuestaGestion","string","MotivoRespuestaGestion","string")
        ], transformation_ctx="ApplyMapping_node2"
) 

# Script generated for node m_sar
map_sar = ApplyMapping.apply(
    frame=df_sar_mkf, mappings=
        [
        ("GrupoFamiliar","string","GrupoFamiliar","string"),
        ("MotivoServicio","string","MotivoServicio","string")
        ], transformation_ctx="ApplyMapping_node2"
) 

# ------------------------------------------------------------ End Mapping data --------------------------------------------------------------------------
# ------------------------------------------------------------ Data Engineering --------------------------------------------------------------------------
df_afil = map_df_afiliaciones.toDF()
df_mkf = map_mkf.toDF()
df_sar = map_sar.toDF()

#today = date.today()

#anio_mes = (today.replace(day=1)).replace(day=1).strftime(format='%Y%m')
#anio_mes_ant = (today.replace(day=1) - timedelta(1)).replace(day=1).strftime(format='%Y%m')
#anio_mes_12_ant = (today.replace(day=1) - timedelta(360)).replace(day=1).strftime(format='%Y%m')

#filtro quitarlo, solo pruebas
#filter_df_afil = df_afil.where((F.col("CODIGO_ESTADO_AFILIADO").isin(['E0003','E0004'])) & (F.col("TIP_AFIL") == "I") & (F.date_format(F.col("FEC_AFIL_CONVERSION"),"yyyyMM") >= "202210") & (F.date_format(F.col("FEC_AFIL_CONVERSION"),"yyyyMM") <= anio_mes_ant) & (F.substring(F.col("NUM_POS"),3,2) == "00"))

#filter_df_afil = filter_df_afil.filter(F.col("FEC_CONTINUIDAD").isNull() & F.col("FEC_RENOVACION").isNull())

# ------------------------------------------------------------ Data transforms --------------------------------------------------------------------------
df_mkf = df_mkf.withColumn("GRUPO_FAMILIAR", lpad(F.col("GrupoFamiliar"),10, "0000000000"))
df_mkf = df_mkf.select("GRUPO_FAMILIAR","FechaGestion","NumeroAfiliados","Periodo1erCobro","MontoPendiente","TipoGestion","NumeroCuotasPendientes","LlamadasXEjecutar","LlamadasEjecutadas","LlamadasValidas","LlamadasContestadas")

df_sar = df_sar.withColumn("GF", lpad(F.col("GrupoFamiliar"),10, "0000000000"))
df_sar = df_sar.select("GF","MotivoServicio")

#aqui quede
join_df_mkf = df_mkf.join(df_sar,(df_mkf.GRUPO_FAMILIAR == df_sar.GF),"left")

join_df_mkf = join_df_mkf.select("GRUPO_FAMILIAR","FechaGestion","NumeroAfiliados","Periodo1erCobro","MontoPendiente","TipoGestion","NumeroCuotasPendientes","MotivoServicio","LlamadasXEjecutar","LlamadasEjecutadas","LlamadasValidas","LlamadasContestadas")

#data en duro para pruebas
df_mkf = join_df_mkf.withColumn("nro_anio_primer_cobro",date_format(F.col("Periodo1erCobro"),"yyyy").cast('int')) \
               .withColumn("nro_mes_primer_cobro",date_format(F.col("Periodo1erCobro"),"MM").cast('int')) \
               .withColumn("DES_SEMANA",F.concat(F.lit("Semana "),F.weekofyear(F.col("FechaGestion")))) \
               .withColumn("semana",F.date_format(F.col("FechaGestion"),"w")) \
               .withColumn("SEMANA_DEL_MES", F.date_format(F.col("FechaGestion"), "W")) \
               .withColumn("dia_sem",F.dayofweek(F.col("FechaGestion"))) \
               .withColumn("dia_mes",F.dayofmonth(F.col("FechaGestion"))) \
               .withColumn("first_day",F.lit(F.col("dia_mes") - F.col("dia_sem") + 1)) \
               .withColumn("last_day",F.lit(F.col("first_day") + 6)) \
               .withColumn("des_periodo_gestion",date_format(F.col("FechaGestion"),"yyyyMM")) \
               .withColumn("fec_inicio_semana_gestion",F.concat(F.concat(F.concat(F.concat(F.col("first_day"),F.lit("-")),date_format(F.col("FechaGestion"),"MM")),F.lit("-")),date_format(F.col("FechaGestion"),"yyyy"))) \
               .withColumn("fec_fin_semana_gestion",F.concat(F.concat(F.concat(F.concat(F.col("last_day"),F.lit("-")),date_format(F.col("FechaGestion"),"MM")),F.lit("-")),date_format(F.col("FechaGestion"),"yyyy"))) 

#CONTINUAR AQUI
df_mkf = df_mkf.withColumn("MTO_PENDIENTE_SEMANA1",F.when(F.col("SEMANA_DEL_MES") >= 1, F.col("MontoPendiente")).otherwise(F.lit("0"))) \
               .withColumn("MTO_PENDIENTE_SEMANA2",F.when(F.col("SEMANA_DEL_MES") >= 2, F.col("MontoPendiente")).otherwise(F.lit("0"))) \
               .withColumn("MTO_PENDIENTE_SEMANA3",F.when(F.col("SEMANA_DEL_MES") >= 3, F.col("MontoPendiente")).otherwise(F.lit("0"))) \
               .withColumn("MTO_PENDIENTE_SEMANA4",F.when(F.col("SEMANA_DEL_MES") >= 4, F.col("MontoPendiente")).otherwise(F.lit("0"))) \
               .withColumn("MTO_PENDIENTE_SEMANA5",F.when(F.col("SEMANA_DEL_MES") >= 5, F.col("MontoPendiente")).otherwise(F.lit("0"))) \
               .withColumn("NRO_CUOTAS_PENDIENTES_SEMANA1",F.when(F.col("SEMANA_DEL_MES") >= 1,F.col("NumeroCuotasPendientes")).otherwise(F.lit(0))) \
               .withColumn("NRO_CUOTAS_PENDIENTES_SEMANA2",F.when(F.col("SEMANA_DEL_MES") >= 2,F.col("NumeroCuotasPendientes")).otherwise(F.lit(0))) \
               .withColumn("NRO_CUOTAS_PENDIENTES_SEMANA3",F.when(F.col("SEMANA_DEL_MES") >= 3,F.col("NumeroCuotasPendientes")).otherwise(F.lit(0))) \
               .withColumn("NRO_CUOTAS_PENDIENTES_SEMANA4",F.when(F.col("SEMANA_DEL_MES") >= 4,F.col("NumeroCuotasPendientes")).otherwise(F.lit(0))) \
               .withColumn("NRO_CUOTAS_PENDIENTES_SEMANA5",F.when(F.col("SEMANA_DEL_MES") >= 5,F.col("NumeroCuotasPendientes")).otherwise(F.lit(0)))

df_afil = df_afil.select("COD_GRUPO_FAMILIAR","PROGRAMA","DES_OFICINA_VENTA","CANAL_DIST","FEC_PRIMER_COBRO","SEDE_COMERCIAL","GRUPO_VENDEDOR","NOMBRE_VENDEDOR","segmento_comercial","des_producto","frecuencia_pago","FEC_AFIL_CONVERSION","FEC_AFIL_DESDE").distinct()

tf_df_afil=df_afil.na.fill("",["segmento_comercial"]) \
                         .na.fill("",["SEDE_COMERCIAL"]) \
                         .na.fill("",["NOMBRE_VENDEDOR"])
                         
tf_df_afil=tf_df_afil.withColumn("DES_SEGMENTO",F.when(F.col("segmento_comercial")=="","Sin segmento").otherwise(F.substring(F.col("segmento_comercial"),4,15))) \
                     .withColumn("DES_SEDE",F.when(F.col("SEDE_COMERCIAL") == "","SIN INFORMACIÓN").otherwise(F.col("SEDE_COMERCIAL"))) \
                     .withColumn("COD_GRUPO_FAM", lpad(F.col("COD_GRUPO_FAMILIAR"),10, "0000000000")) \
                     .withColumn("DES_ANTIGUEDAD_APORTANTE",F.round(F.months_between(F.current_date(),F.col("FEC_AFIL_DESDE")),0)) \
                     .withColumn("DES_TIPO_PAGO",F.lit("F")) \
                     .withColumn("COORDINADOR",F.lit("F"))
                     

join_df_result = df_mkf.join(tf_df_afil,(df_mkf.GRUPO_FAMILIAR == tf_df_afil.COD_GRUPO_FAM),"left")

df_result = join_df_result.select(F.col("COD_GRUPO_FAMILIAR"),
                               F.col("TIPOGESTION").alias("TIP_GESTION"),
                               F.col("CANAL_DIST").alias("DES_CANAL"),
                               F.col("DES_OFICINA_VENTA"),
                               F.col("GRUPO_VENDEDOR").alias("DES_GRUPO_VENDEDOR"),
                               F.col("NumeroAfiliados").alias("NRO_AFILIADOS").cast('int'),
                               F.col("DES_ANTIGUEDAD_APORTANTE").cast('int'),
                               F.col("des_producto").alias("DES_UNI_NEGOCIO"),
                               F.col("PROGRAMA").alias("DES_PROGRAMA"),
                               F.col("DES_SEGMENTO"),
                               F.col("DES_TIPO_PAGO"),
                               F.col("frecuencia_pago").alias("DES_FRECUENCIA_TARIFA"),
                               F.col("NRO_ANIO_PRIMER_COBRO"),
                               F.col("NRO_MES_PRIMER_COBRO"),
                               F.col("DES_SEDE"),
                               F.col("COORDINADOR").alias("DES_SUPERVISOR"),
                               F.col("NOMBRE_VENDEDOR").alias("DES_VENDEDOR"),
                               F.col("FechaGestion"),
                               F.col("MontoPendiente").alias("NRO_CUOTAS_PENDIENTES_INICIO_MES").cast("decimal(10,2)"),
                               F.col("MTO_PENDIENTE_SEMANA1").cast("decimal(10,2)"),
                               F.col("MTO_PENDIENTE_SEMANA2").cast("decimal(10,2)"),
                               F.col("MTO_PENDIENTE_SEMANA3").cast("decimal(10,2)"),
                               F.col("MTO_PENDIENTE_SEMANA4").cast("decimal(10,2)"),
                               F.col("MTO_PENDIENTE_SEMANA5").cast("decimal(10,2)"),
                               F.col("NRO_CUOTAS_PENDIENTES_SEMANA1").cast("int"),
                               F.col("NRO_CUOTAS_PENDIENTES_SEMANA2").cast("int"),
                               F.col("NRO_CUOTAS_PENDIENTES_SEMANA3").cast("int"),
                               F.col("NRO_CUOTAS_PENDIENTES_SEMANA4").cast("int"),
                               F.col("NRO_CUOTAS_PENDIENTES_SEMANA5").cast("int"),
                               F.col("LlamadasXEjecutar").alias("NRO_LLAMADAS_POR_EJECUTAR").cast("int"),
                               F.col("LlamadasEjecutadas").alias("NRO_LLAMADAS_EJECUTADAS").cast("int"),
                               F.col("LlamadasContestadas").alias("NRO_LLAMADAS_CONTESTADAS").cast("int"),
                               F.col("LlamadasValidas").alias("NRO_LLAMADAS_VALIDAS").cast("int"),
                               F.col("MotivoServicio").alias("DES_TIPIFICACION_CASO"),
                               F.col("DES_PERIODO_GESTION"),
                               F.col("FEC_INICIO_SEMANA_GESTION").cast("date"),
                               F.col("FEC_FIN_SEMANA_GESTION").cast("date"),
                               F.col("DES_SEMANA"),
                               F.lit(F.current_date()).alias("FEC_CARGA"))
                                                    

print(df_result.show(50))
print(df_result.count())
df_result.printSchema()

print(df_mkf.show(50))

#guardado
df_result.write.mode('overwrite') \
		 .format('parquet') \
		 .save('s3://auna-dlaqa-stage-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf_cuotas_gestionadas/')


