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
# Script generated for node S3 bucket
df_afiliaciones = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://auna-dlaprd-analytics-s3/structured-data/OLAP/dwh-Modelos-Operacional-Poblaciones/pry-mop-Poblacion-Oncosalud/poblacion-afiliaciones/"
        ],
        "recurse": True,
    },
    #transformation_ctx="S3bucket_node1",
)

df_historica_activas = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://auna-dlaprd-stage-s3/structured-data/OLAP/poblacion-historica-oncosalud/activas/202111/",
            "s3://auna-dlaprd-stage-s3/structured-data/OLAP/poblacion-historica-oncosalud/activas/202112/",
            "s3://auna-dlaprd-stage-s3/structured-data/OLAP/poblacion-historica-oncosalud/activas/202201/",
            "s3://auna-dlaprd-stage-s3/structured-data/OLAP/poblacion-historica-oncosalud/activas/202202/",
            "s3://auna-dlaprd-stage-s3/structured-data/OLAP/poblacion-historica-oncosalud/activas/202203/",
            "s3://auna-dlaprd-stage-s3/structured-data/OLAP/poblacion-historica-oncosalud/activas/202204/"
        ],
        "recurse": True,
    },
)

df_historica_bajas = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://auna-dlaprd-stage-s3/structured-data/OLAP/poblacion-historica-oncosalud/bajas/"
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
        ("NUM_POS","string", "NUM_POS", "string"),
        ("COD_GRUPO_FAMILIAR", "string", "COD_GRUPO_FAMILIAR", "string"),
        ("RANGO_EDAD_CONTRATANTE", "string", "RANGO_EDAD_CONTRATANTE", "string"),
        ("PROGRAMA", "string", "PROGRAMA", "string"),
        ("DES_OFICINA_VENTA", "string", "DES_OFICINA_VENTA", "string"),
        ("CANAL_DIST", "string", "CANAL_DIST", "string"),
        ("COD_FREC_PAGO", "string", "COD_FREC_PAGO", "string"),
        ("COD_TIP_PAGO", "string", "COD_TIP_PAGO", "string"),
        ("ESTADO_AFILIADO", "string", "ESTADO_AFILIADO", "string"),
        ("FEC_PRIMER_COBRO", "date", "FEC_PRIMER_COBRO", "date"),
        ("FEC_EJEC_BAJA_AFIL", "date", "FEC_EJEC_BAJA_AFIL", "date"),
        ("FEC_AFIL_CONVERSION", "date", "FEC_AFIL_CONVERSION", "date"),
        ("SEDE_COMERCIAL", "string", "SEDE_COMERCIAL", "string"),
        ("GRUPO_VENDEDOR", "string", "GRUPO_VENDEDOR", "string"),
        ("NOMBRE_VENDEDOR", "string", "NOMBRE_VENDEDOR", "string"),
        ("segmento_comercial", "string", "segmento_comercial", "string"),
        ("CODIGO_ESTADO_AFILIADO", "string", "CODIGO_ESTADO_AFILIADO", "string"),
        ("FEC_CONTINUIDAD", "date", "FEC_CONTINUIDAD", "date"),
        ("FEC_RENOVACION", "date", "FEC_RENOVACION", "date"),
        ("TIP_AFIL", "string", "TIP_AFIL", "string"),
    ],
)

map_df_historica_activas = ApplyMapping.apply(
    frame=df_historica_activas,
    mappings=[
        ("cod_cliente", "string", "cod_cliente", "string"),
        ("Cod_Grupo_Familiar", "string", "Cod_Grupo_Familiar", "string"),
        ("CodDoc", "string", "CodDoc", "string"),
        ("des_programa", "string", "des_programa", "string"),
        ("rng_edad", "string", "rng_edad", "string"),
        ("perfil_bi", "string", "perfil_bi", "string"),
        ("tipo_pago", "string", "tipo_pago", "string"),
        ("cosecha", "string", "cosecha", "string"),
        ("asesor", "string", "asesor", "string"),
        ("segmecanal", "string", "segmecanal", "string"),
        ("tipo_afiliacion", "string", "tipo_afiliacion", "string"),
        ("frecuencia", "string", "frecuencia", "string"),
        ("modo", "string", "modo", "string"),
        ("fecha_continuidad", "date", "fecha_continuidad", "date"),
        ("fecha_renovacion", "date", "fecha_renovacion", "date")
    ],
)

map_df_historica_bajas = ApplyMapping.apply(
    frame=df_historica_bajas,
    mappings=[
        ("Cod_Cliente", "string", "Cod_Cliente", "string"),
        ("Cod_Grupo_Familiar", "string", "Cod_Grupo_Familiar", "string"),
        ("Periodo", "string", "Periodo", "string")
    ],
)

# ------------------------------------------------------------ End Mapping data --------------------------------------------------------------------------
# ------------------------------------------------------------ Data Engineering --------------------------------------------------------------------------
df_afil = map_df_afiliaciones.toDF()
df_ha = map_df_historica_activas.toDF()
df_hb = map_df_historica_bajas.toDF()

today = date.today()

anio_mes = (today.replace(day=1)).replace(day=1).strftime(format='%Y%m')
anio_mes_ant = (today.replace(day=1) - timedelta(1)).replace(day=1).strftime(format='%Y%m')
anio_mes_12_ant = (today.replace(day=1) - timedelta(360)).replace(day=1).strftime(format='%Y%m')

#acordarse poner la fecha correcta de anio_mes_12_ant
filter_df_afil = df_afil.where((F.col("CODIGO_ESTADO_AFILIADO").isin(['E0003','E0004'])) & (F.col("TIP_AFIL") == "I") & (F.date_format(F.col("FEC_AFIL_CONVERSION"),"yyyyMM") >= anio_mes_12_ant) & (F.date_format(F.col("FEC_AFIL_CONVERSION"),"yyyyMM") <= anio_mes_ant) & (F.substring(F.col("NUM_POS"),3,2) == "00"))

filter_df_afil = filter_df_afil.filter(F.col("FEC_CONTINUIDAD").isNull() & F.col("FEC_RENOVACION").isNull())

filter_df_ha = df_ha.where((F.col("tipo_afiliacion") == "I") & (F.col("frecuencia") == "MENSUAL") & (F.col("modo") == "RECURRENTE") & (F.col("cosecha") >= anio_mes_12_ant) & (F.col("cosecha") < "202205"))

filter_df_ha = filter_df_ha.filter(F.col("fecha_continuidad").isNull() & F.col("fecha_renovacion").isNull())

filter_df_hb = df_hb.where((F.col("Periodo") > anio_mes_12_ant) & (F.col("Periodo") < "202205"))

# ------------------------------------------------------------ Data transforms --------------------------------------------------------------------------
tf_df_afil=filter_df_afil.na.fill("",["segmento_comercial"]) \
                            .na.fill("",["SEDE_COMERCIAL"]) \
                            .na.fill("",["NOMBRE_VENDEDOR"]) \
                            .na.fill("",["RANGO_EDAD_CONTRATANTE"])  

#RECORDAR QUITAR LOS LIT DE TIPO PAGO Y COORDINADOR
tf_df_afil=tf_df_afil.withColumn("DES_SEGMENTO",F.when(F.col("segmento_comercial")=="","Sin segmento").otherwise(F.substring(F.col("segmento_comercial"),4,15))) \
                     .withColumn("DES_SEDE",F.when(F.col("SEDE_COMERCIAL") == "","SIN INFORMACIÓN").otherwise(F.col("SEDE_COMERCIAL"))) \
                     .withColumn("DES_RANGO_ETAREO",F.when(F.col("RANGO_EDAD_CONTRATANTE") == "","OTROS").otherwise(F.col("RANGO_EDAD_CONTRATANTE"))) \
                     .withColumn("DES_TIPO_PAGO",F.lit("")) \
                     .withColumn("COORDINADOR",F.lit(""))

tf_df_altas = filter_df_ha.select("cod_cliente","cod_cliente","Cod_Grupo_Familiar","cosecha","CodDoc","des_programa","rng_edad","perfil_bi","tipo_pago","segmecanal","asesor").distinct()

tf_df_altas = tf_df_altas.withColumn("asesor",F.when((F.col("asesor") == F.lit("RODRIGUEZ SANCHEZ DE SALVATIERRA,  BEATRIZ D'FIORELLA")) | (F.col("asesor") == F.lit("RODRIGUEZ SANCHEZ, BEATRIZ D'FIORELLA")),F.lit("RODRIGUEZ SANCHEZ DE SALVATIERRA, BEATRIZ D FIORELLA")).otherwise(F.col("asesor")))

tf_df_altas = tf_df_altas.withColumn("Cod_GF", lpad(F.col("Cod_Grupo_Familiar"),10, "0000000000")) \
                         .withColumn("FechaAlta",F.concat(F.concat(F.concat(F.substring(F.col("cosecha"),1,4),F.lit("-")),F.substring(F.col("cosecha"),5,2)),F.lit("-01")).cast('date')) \
                         .withColumn("DES_CANAL",F.expr("case when segmecanal IN ('CANAL DIGITAL','Digital') then 'Digital' " +
                                                                "when segmecanal = 'Banca & Seguros' Then 'Alianzas' " +
                                                                "When segmecanal IN ('CANAL DIRECTO','CANAL INDIRECTO','FFVV Directa','FFVV Indirecta') Then 'Fuerza de Ventas' " +
                                                                "When segmecanal = 'Corporativo Individual' Then 'Corporativo Individual' " +
                                                                "When segmecanal IN ('GRUPAL','Grupal') Then 'Grupal' " +
                                                                "When segmecanal = 'NEGOCIO MULTICANAL' Then 'Negocio Multicanal' " +
                                                                "When segmecanal IN ('PLANES SALUD','Planes Salud') Then 'Planes de Salud' " +
                                                                "When segmecanal IN ('TELEMARKETING','Telemarketing.') Then 'Planes de Salud' " +
                                                                "When segmecanal = 'WORKSITE' Then 'Empresas' end")) \
                         .withColumn("DES_OFICINA_VENTA",F.expr("case when DES_CANAL = 'Alianzas' then 'Banca Seguros' " +
                                                                     "when DES_CANAL = 'Digital' Then 'E-commerce' " +
                                                                     "when DES_CANAL = 'Fuerza de Ventas' AND segmecanal IN ('CANAL DIRECTO','FFVV Directa') Then 'Directo' " +
                                                                     "when DES_CANAL = 'Fuerza de Ventas' AND segmecanal IN ('CANAL INDIRECTO','FFVV Indirecta') Then 'Indirecto' " +
                                                                     "when DES_CANAL = 'Corporativo Individual' Then 'Corporativo Individual' " +
                                                                     "when DES_CANAL = 'Grupal' Then 'Grupal' " +
                                                                     "when DES_CANAL = 'Negocio Multicanal' Then 'Negocio Multicanal' " +
                                                                     "when DES_CANAL = 'Planes de Salud' Then 'Planes de Salud' " +
                                                                     "when DES_CANAL = 'Telemarketing.' Then 'Telemárketing Extern' " +
                                                                     "when DES_CANAL = 'Empresas' Then 'Equipo Worksite' end")) \
                         .withColumn("DES_GRUPO_VENDEDOR",F.expr("case when DES_OFICINA_VENTA = 'Banca Seguros' then 'Piso Banca' " +
                                                                     "when DES_OFICINA_VENTA = 'E-commerce' Then 'E-commerce' " +
                                                                     "when DES_CANAL = 'Fuerza de Ventas' AND DES_OFICINA_VENTA = 'Directo' Then 'Directo 1' " +
                                                                     "when DES_CANAL = 'Fuerza de Ventas' AND DES_OFICINA_VENTA = 'Indirecto' Then 'Libre' " +
                                                                     "when DES_OFICINA_VENTA = 'Corporativo Individual' Then 'Corporativo Individual' " +
                                                                     "when DES_OFICINA_VENTA = 'Grupal' Then 'Grupal' " +
                                                                     "when DES_OFICINA_VENTA = 'Negocio Multicanal' Then 'Negocio Multicanal' " +
                                                                     "when DES_OFICINA_VENTA = 'Planes de Salud' Then 'Planes de Salud' " +
                                                                     "when DES_OFICINA_VENTA = 'Telemárketing Extern' Then 'Base' " +
                                                                     "when DES_OFICINA_VENTA = 'Equipo Worksite' Then 'Directo WS' end")) \
                         .withColumn("DES_COORDINADOR",F.expr("Case When asesor = 'ÑIQUE TOCAS,  SHIRLEY MASSIEL' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'ACUÑA LARA,  SOCORRO MAGDALENA' Then 'HERNANDEZ MORENO,  GRACIELA EUGENIA' " +
                                                            "When asesor = 'AFILIACION ,  VIRTUAL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'AFILIACION , VIRTUAL ' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'AGURTO BOUILLON,  CINTHIA MARIA' Then 'SALAS, FARLYN' " +
                                                            "When asesor = 'AITA DE CHIRINOS,  ELSA MARIA' Then 'GALLEGOS VALDEZ,  MARY ANN' " +
                                                            "When asesor = 'AITA VDA DE CHIRINOS,  ELSA MARIA' Then 'GALLEGOS VALDEZ,  MARY ANN' " +
                                                            "When asesor = 'ALARCON FLORES,  JACQUELINE YAHAIRA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ALARCON ROJAS,  GRECIA ELIZABETH' Then 'LLERENA, MARCO' " +
                                                            "When asesor = 'ALARCON VASQUEZ,  MARIA MAGDALENA' Then 'ARBULU, DARLING' " +
                                                            "When asesor = 'ALAYO JIMENEZ DE VILCHEZ,  KARINA CONCEPCION' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ALBAN VILLEGAS,  ALEYDA VIANEY' Then 'SALAS, FARLYN' " +
                                                            "When asesor = 'ALCANTARA MORENO,  MARIA FIORELLA' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'ALFARO ASTETE,  FIORELLA YULIE' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'ALFARO AVALOS,  JACKELINE' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'ALMESTAR MAURICIO DE CARRERA,  VANESA' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'ALT GROUP S.A.C.' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'ALVARADO CHERRE,  ALI ARMANDO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ALVARADO FERNANDEZ,  JOHANNA MILAGRITOS' Then 'ROJAS,  ZAIDVICT' " +
                                                            "When asesor = 'ALVARADO LIZAMA,  JACKELINE MIRELLA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ALVAREZ RAMOS,  JULIANA ELIZABETH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ALVAREZ SALERNO,  JUAN CARLOS' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'ALVEAR LLALLA,  CAROLINA' Then 'EGOAVIL, JOEL' " +
                                                            "When asesor = 'ALZAMORA CASTILLO,  BRAYAN YAMPIER' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ALZAMORA MORON DE GIL,  MARIA DEL ROSARIO' Then 'PACHECO, ZAIDA' " +
                                                            "When asesor = 'ALZOLA COHEN,  JOHANNA ALEXANDRA' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'AMARO MAMANI,  SANDRA YSAVET' Then 'GRANDA, ROY' " +
                                                            "When asesor = 'ANACLETO JIMÉNEZ,  PATRICIA ELIANA' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'ANACLETO JIMENEZ,  PATRICIA ELIANA' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'ANCAJIMA MELENDEZ,  FLOR DE MARIA' Then 'ROMERO ALCEDO,GINA ORFELINDA' " +
                                                            "When asesor = 'ANCIBURO FELIX,  ROSA MATILDE' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'ANDRADE ZUÑIGA,  PABLO CESAR' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ANGULO CUEVA,  ROSA ISABEL' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'ANSHIN S.A.C. CORREDORES DE SEGUROS' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'ANTICONA VELASQUEZ,  VANESSA KATHERINE' Then 'ANTICONA VELASQUEZ,  VANESSA KATHERINE' " +
                                                            "When asesor = 'ANTONIO CUADROS,  CINTYA NATALY' Then 'LLERENA, MARCO' " +
                                                            "When asesor = 'ARAGON CASTILLO,  VERONICA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ARANIBAR ACUÑA,  DAYANARA ZANAE' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'ARANIS GARCIA - ROSSELL,  GABRIEL ANDRES' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ARCE DEL BUSTO,IRIS CAROLINA' Then 'PYE SOLARI DE GONZALEZ,  JANE LOUISE' " +
                                                            "When asesor = 'ARESTEGUI CANALES DE RENGIFO,  SONIA' Then 'PURIZAGA RAMOS,FANNY ELENA' " +
                                                            "When asesor = 'ARROYO RUBIO,  MELISSA BERENISSE' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'ARROYO ZAVALETA,  ANGEL OSWALDO' Then 'VENTURO, NADIA' " +
                                                            "When asesor = 'ARRUE COLLAHUA,  MELANIE AMY ELIZABETH' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'ARTHUR J. GALLAGHER PERÚ CORREDORES DE SEGUROS S.A.' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'ASENCIOS ESPINOZA,  GLORIA MARIA' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'ASENCIOS ESPINOZA,GLORIA MARIA' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'ASENJO FERREYROS,  RENZO ALBERTO' Then 'Sin Coordinador' " +
                                                            "When asesor = 'asesor ECOMMERCE,  .' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'ASTETE LOPEZ, LEASOL ALICIA' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'ATLANTIC CORREDORES DE SEGUROS S.A.C.' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'AVALOS PAZ, YOLANDA BEATRIZ' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'AVELLANEDA GUZMAN,  YOVANNY DEL PILAR' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'AVENDAÑO RODRIGUEZ,  ELIZABETH GIANNINA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'AYSANOA BALLESTER,  ELIAS RODOLFO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'AYULO PALACIOS,  ANGELA CESAREA' Then 'ROJAS,  ZAIDVICT' " +
                                                            "When asesor = 'BACAS QUISPE,ZENAIDA ' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'BALCAZAR FLORES,  MARLENY NADIA' Then 'CIEZA, LILIANA' " +
                                                            "When asesor = 'BAMONDE SEGURA,  CARMEN ADELA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BARDALEZ FLORES,  JANE LEE' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'BARRANTES COLETTI,  ANA MARIA' Then 'TRIGOSO, YISEL' " +
                                                            "When asesor = 'BARRANTES PAREDES,  LILIAN REGINA' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'BARRANTES PAREDES, LILIAN REGINA' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'BARRIENTOS SILVA,  JACKELINE' Then 'NIMA, SILVIA' " +
                                                            "When asesor = 'BARROS DOMINGUEZ,  KAREN MARILU' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BASTANTE LAZARO,  MARIA ESTHER' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'BASTARRACHEA GARCIA,  EMMA JESUS' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BASTOS NINAYAHUAR,  CARMEN ISELA' Then 'NOVOA, LLANDER' " +
                                                            "When asesor = 'BASURTO ARAKI,  EDUARDO' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'BAY DE LAZO,  VICTORIA HORTENSIA' Then 'ANTICONA VELASQUEZ,  VANESSA KATHERINE' " +
                                                            "When asesor = 'BAY OLAECHEA VDA DE LAZO,  VICTORIA HORTENSIA' Then 'ANTICONA VELASQUEZ,  VANESSA KATHERINE' " +
                                                            "When asesor = 'BAYONA FLORES,  MARIA DEL CARMEN' Then 'PERALTA, DANIELA' " +
                                                            "When asesor = 'BECERRA BROKERS CORREDORES DE SEGUROS SAC' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'BECERRA CUEVA,  VANESA SOFIA' Then 'GOMEZ, JESSICA' " +
                                                            "When asesor = 'BEDON SARANGO,  SISSY MELISSA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BEDREGAL GARCIA,  MARYSABEL VICTORIA' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'BEJARANO AGUINAGA,  DIEGO ALONSO' Then 'GRANDA, ROY' " +
                                                            "When asesor = 'BEJARANO BALCAZAR, JULIA ELIZABETH' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'BELTRAME MONTOYA,  JORGE RAUL' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'BELTRAN LAVANDERA,  MARILYN YOLANDA' Then 'BELTRAN LAVANDERA,  MARILYN YOLANDA' " +
                                                            "When asesor = 'BENAVENTE LEIGH,  MARY SILVIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BENAVIDES AYALA,  ALEX MICHAEL' Then 'MOGOLLON, SANDRA' " +
                                                            "When asesor = 'BENAVIDES VERGARA,CLARA MARIA IRMA ANA' Then 'CARBAJAL LIZARRAGA,MONICA DEL SOCORRO' " +
                                                            "When asesor = 'BENGOA MARQUINA,  ANELIN MERCEDES' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'BERMEJO GARCIA,  GIULIANA PAOLA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BERNAL TORRES,  NERY DAMARIS' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'BERNUY SILVA,  ROSA JEANNETTE' Then 'PEREYRA DE MORALES,  OLINDA FIDELIA' " +
                                                            "When asesor = 'BIANCHI BURGA,GIANNINA MARIA DEL CARMEN' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'BOCANEGRA MONGRUT,  CRISTIAN ALEJANDRO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BOCANEGRA NUÑEZ DE ANGULO,  YESENIA ROXANA' Then 'ROJAS,  ZAIDVICT' " +
                                                            "When asesor = 'BOHORQUEZ HAUYON,  VICTOR ALONSO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BONIFAZ URETA,  MAURICIO GABRIEL' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'BORDA VEGA,  PATRICIA' Then 'CUADROS,  PAOLA' " +
                                                            "When asesor = 'BORJA ALEJOS,  MONICA CECILIA' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'BOTTON PANTA,  HELGA MARIELLA' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'BOULANGER JIMENEZ,  FIORELLA DEL CARMEN' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BRACAMONTE UGAZ,  LOURDES GORETTI' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'BRAVO HERENCIA,  CARLOS ENRIQUE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BRENNER GOMEZ,  ISABEL CRISTINA' Then 'CIEZA, LILIANA' " +
                                                            "When asesor = 'BRETONECHE ,  MERCEDES MARISELLA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BRUNO CORDOVA,  FRESSIA DEL PILAR' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BRUNO CORDOVA, FRESSIA DEL PILAR' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'BUENO RODRIGUEZ,  GLADYS ROSMERY' Then 'PEREYRA DE MORALES,  OLINDA FIDELIA' " +
                                                            "When asesor = 'BUITRON LOLY,  CARMEN ROSARIO' Then 'BUITRON LOLY,  CARMEN ROSARIO' " +
                                                            "When asesor = 'BURGA BRAVO,  JULIA MARIBEL' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'BURGOS IBAÑEZ,  CINTHIA VANESSA' Then 'NIMA, SILVIA' " +
                                                            "When asesor = 'BUSTAMANTE PRADA,  CESAR AUGUSTO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CABANA DIAZ,  DANICA GABRIELA' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'CABEZAS ZAMUDIO,  LORENA MARINA' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'CABRERA CARRILLO,  DIEGO ARTURO' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'CABRERA MATUTE,  NIDIA SORAYA' Then 'CABRERA MATUTE,  NIDIA SORAYA' " +
                                                            "When asesor = 'CABRERA MIÑANO,  MARIA DEL PILAR' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'CACERES VARGAS,CARMEN ASTRID' Then 'ISHARA NAKASONE,JULIA BEATRIZ' " +
                                                            "When asesor = 'CACHAY SAAVEDRA,  EVELIN JANETT' Then 'TRIGOSO, YISEL' " +
                                                            "When asesor = 'CAJUSOL CHEPE,ROSA NEGDA' Then 'CAJUSOL CHEPE,ROSA NEGDA' " +
                                                            "When asesor = 'CALDAS VALVERDE,  ORFA TABITA' Then 'PACHECO, ZAIDA' " +
                                                            "When asesor = 'CALDAS ZAPATA,  JORGE RAUL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CALDERON VASQUEZ,  LIDYA LISSETTE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CALLE TAVARA,  PATRICIA MARIA DEL CARMEN' Then ', VACANTE PIURA' " +
                                                            "When asesor = 'CAMPOS ARAMBULO,  JHONY LEONEL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CAMPOS CASTILLO,  VANESSA' Then 'ANTICONA VELASQUEZ,  VANESSA KATHERINE' " +
                                                            "When asesor = 'CAMPOS MORENO,  SANDRA CONSUELO' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'CANAL ,  RECUPERO INDIRECTO NR' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CANAL RECUPERO,  DIRECTO NR' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CANAL RECUPERO, DIRECTO NR ' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CAQUI TELLO,  JOSE MIGUEL' Then 'PERALTA, DANIELA' " +
                                                            "When asesor = 'CARDOZA LICAS,  VALERY ROXANA' Then 'NAVARRETE, DORA' " +
                                                            "When asesor = 'CARDOZO HONORIO,  MAYRELLI GEANNI' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CARNERO MORA,  ELVIRA VICTORIA' Then 'TRIGOSO, YISEL' " +
                                                            "When asesor = 'CARO AQUINO,  CESAR MANUEL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CARO SALAZAR DE GUANILO,  MILAGRITOS CINTHIA ELIZABETH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CARPIO QUINTASI,  REDY MAURICIO' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'CARPIO VELARDE,  ROCIO MIRYAM' Then 'CUADROS,  PAOLA' " +
                                                            "When asesor = 'CARRANZA CASTAÑEDA,MARGOT ' Then 'ROMERO ALCEDO,GINA ORFELINDA' " +
                                                            "When asesor = 'CARRANZA RAMIREZ,  JAIME GREGORIO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CARRASCO CORRALES,  ANA CECILIA' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'CARRILLO LEANDRO,  MARIA DEL ROSARIO' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'CARRION GUEVARA,  KIHARA STEPHANY' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'CARRION SEBASTIANI,  LOURDES DEL CARMEN' Then 'GRANIZO, ALEXANDRA' " +
                                                            "When asesor = 'CASTAÑEDA CERCADO,AGUEDA ' Then 'PYE SOLARI DE GONZALEZ,  JANE LOUISE' " +
                                                            "When asesor = 'CASTILLO CRUZ,  SANDY KATHERINE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CASTILLO HERNANDEZ,  JAIME RICHARD' Then 'GRANDA, ROY' " +
                                                            "When asesor = 'CASTILLO HUAMAN,  YOVANI ESPERANZA' Then 'TRIGOSO, YISEL' " +
                                                            "When asesor = 'CASTILLO JHON,  ADA JACKELYN' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CASTILLO MIMBELA,  GISELLA YVETTE' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'CASTILLO PISCONTE,  SANDRA DEL PILAR' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CASTILLO SAAVEDRA,  GLADYS TANIA' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'CASTILLO SUSANIBAR,  ANGEL ANTONIO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CASTRAT FIEGE,  MARIA DEL PILAR' Then 'CASTRAT FIEGE,  MARIA DEL PILAR' " +
                                                            "When asesor = 'CASTRO GASTELO,  THALIA CARMENROSA' Then 'MERINO, FRANK' " +
                                                            "When asesor = 'CASTRO RAMOS,  GERALDINE DEL CARMEN' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'CASTRO ZUÑIGA,  GEORGINNA ALEXANDRA' Then 'VENTURO, NADIA' " +
                                                            "When asesor = 'CCAMA MAMANI,  NARVI DENIS' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CCAULLA HUAMACCTO,  YENY AURELIA' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'CELIS VENTURA,  MERSI SUJEY' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'CERNA ZAMORA,  CARMEN VICTORA' Then 'CIEZA, LILIANA' " +
                                                            "When asesor = 'CESPEDES CAJO,  NATALIA DEL JESUS' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CHACON LOPEZ,  KATHERINE FABIOLA' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'CHAMAN CHANG,  MIRTHA DEL ROCIO' Then 'PEREYRA DE MORALES,  OLINDA FIDELIA' " +
                                                            "When asesor = 'CHAMBI LIPE,  MARIBEL BETZABET' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CHANAME BRANDIZZI,  SONIA JOHANNA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CHANG SALAZAR,  JULISSA MARGARETT' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'CHAVEZ FARROMEQUE,  ANDREA NATALIA' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'CHAVEZ HURTADO,  MARIA PAZ' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'CHAVEZ MENDOZA,  FANNY ROXANA' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'CHIMPEN MIMBELA,  RONALD LUIS' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'CHIROQUE GARCIA,  FAYSSULY ELIBETH' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'CHUECA REQUENA,  INGRID GIANNINA' Then 'PISCOYA, SONIA' " +
                                                            "When asesor = 'CHUMPITAZ ORTEGA,  KAROLINE CRISTAL' Then 'PERALTA, DANIELA' " +
                                                            "When asesor = 'CIEZA FERNANDEZ,  NANCY' Then 'VENTURO, NADIA' " +
                                                            "When asesor = 'CISNEROS SCHUAVEZ,  MARIA JOSE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CJURO RAMOS,  MIRIAM MILAGROS' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CODIGO RECURRENTE CANAL,  DIRECTO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CODIGO RECURRENTE CANAL,  INDIRECTO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CODIGO RECURRENTE CANAL, DIRECTO ' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CODIGO RECURRENTE CANAL, INDIRECTO ' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'COICO SUYON,  KATHERIN DOMINGA' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'CONSEJEROS Y CORREDORES DE SEGUROS SAC' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'CORDERO DIAZ,  FIORELLA MILAGROS' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'CORDOVA CHACONDORI,  ANGIE DAISY' Then 'IZAGUIRRE, ROXANA' " +
                                                            "When asesor = 'CORDOVA ESCOBAR,  GLADYS ROSSANA' Then 'RENGIFO RENGIFO,NINI ISABEL' " +
                                                            "When asesor = 'CORDOVA FLORIANO,  SANDY ROSA' Then 'GRANDA, ROY' " +
                                                            "When asesor = 'CORDOVA VICENTE,  GABRIEL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CORRALES BENITES,  CAROL GRACE' Then 'MARQUEZ,  LESLY' " +
                                                            "When asesor = 'CORREA GUEVARA,  ALEXANDER' Then 'INFANTE, ERNESTO' " +
                                                            "When asesor = 'CORREA NORIEGA,  ROSITA FLOR' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'CORREA SAENZ,  DORA JUANA' Then 'ROJAS,  ZAIDVICT' " +
                                                            "When asesor = 'CORREDORES DE SEGUROS FALABELLA S.A.C.' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'COTRINA PALACIOS,  JINNY KATHERINE' Then 'TOVAR, PATRICIA' " +
                                                            "When asesor = 'CRUZ RODRIGUEZ,  DENIA VANESSA' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'CRUZ ZUMAETA,  JOSSELYN GERALDINE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'CUCHO REJAS,  GRESIA GISBEL' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'CUENCA GOMEZ,  SANDRA GEOVANI' Then 'ROJAS,  ZAIDVICT' " +
                                                            "When asesor = 'CUEVA MASABEL,  IVONNE ALEXANDRA' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'CUSTODIO ARTEAGA,  VALERIA ALEXANDRA' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'CUYA CUYA,  STEPHANIE BRISSETTE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'DE LA CRUZ HONORES,  ROSSMARY' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'DE LA CRUZ MEDINA,  MONICA FARAH' Then 'NOVOA, LLANDER' " +
                                                            "When asesor = 'DE LA PUENTE GONZALES DEL RIEGO,  JULIA MARIA' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'DE LA ROSA MOROTE,  MARIA LETICIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'DE LOS RIOS MARQUEZ,  CORINA YASMIN' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'DE LOS RIOS MONCADA,VIOLETA ' Then 'COORDINACION LIBRE, SAN BORJA (1)' " +
                                                            "When asesor = 'DEDUCCION VENTA ,  NO RECURRENTE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'DEDUCCION VENTA ,  RECURRENTE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'DEDUCCION VENTA , NO RECURRENTE ' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'DEL CASTILLO PANCORVO,  JULIA ELENA' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'DEL VALLE PORTAL,  SARA' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'DELGADO AGUILAR,  MARIA GUISELA' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'DELGADO ALVAREZ,  CLAUDIA ALEJANDRA' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'DEZA MENDOZA,  KAREN STEFANY' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'DIAZ CUCHO,  HERBERT ALBERTO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'DIAZ FERNANDEZ,  GLADYS DEL PILAR' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'DIAZ ORIBE,  ROSA MARGARITA' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'DIESTRA ALAYO DE RODRIGUEZ,  MARIA RAQUEL' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'DIESTRA TRINIDAD,  MAYDA VANESA' Then 'INFANTE, ERNESTO' " +
                                                            "When asesor = 'DIOS CISNEROS,  PATRICIA MERCEDES' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'DIOS CISNEROS, PATRICIA MERCEDES' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'DIOSES MONASTERIO,  STEFFANY' Then 'MOGOLLON, SANDRA' " +
                                                            "When asesor = 'DURAN RODRIGUEZ,  MARIA DEL ROSARIO' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'EGO AGUIRRE ESPINOZA,ROSA SARA' Then 'MORAN MAURICIO DE LOYOLA,  MAGDALENA MARIA' " +
                                                            "When asesor = 'ELIAS NAVARRETE DE RIZO PATRON,  ROCIO DEL CARMEN' Then 'CARBAJAL LIZARRAGA,MONICA DEL SOCORRO' " +
                                                            "When asesor = 'EMP DE SERVICIOS MULTIPLES UNIVERSAL S.R.L.' Then 'MERLUZZI, VALLADARES MIRELLA' " +
                                                            "When asesor = 'ESCALANTE CUYA,  ERIKA' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'ESCOBEDO ZUÑIGA,  EDGAR ERNESTO ISAIAS' Then 'LLERENA, MARCO' " +
                                                            "When asesor = 'ESCUDERO VELASQUEZ,NORMA ' Then 'MORAN MAURICIO DE LOYOLA,  MAGDALENA MARIA' " +
                                                            "When asesor = 'ESCURRA POLO,  ELVIRA AMANDA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ESPEJO GONZALES,  JULISSA AIDA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ESPINAL GUTIERREZ,  ALEJANDRA PENELOPE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ESQUERRE QUISPE,  ROSA ISABEL' Then 'IZAGUIRRE, ROXANA' " +
                                                            "When asesor = 'ESQUIVEL CABRERA,  ELIZABETH JOHNNY' Then 'TRIGOSO, YISEL' " +
                                                            "When asesor = 'ESTEBAN ARELLANO,  CARMEN ZENAIDA' Then 'PACHECO, ZAIDA' " +
                                                            "When asesor = 'ESTELA TORRES,  RAQUEL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FABIAN MORI,  TERESA' Then 'DIAZ GARCIA,  ELTON JHON' " +
                                                            "When asesor = 'FALCON RAMIREZ,  PAOLA CONSUELO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FALZONE ESPINOZA,  NICOLE JOLIE' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'FARFAN GRANADINO,  FIORELLA YESENIA' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'FARFAN URDANEGUI,  KATHERINE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FARIAS ARCA,  IRMA ELIZABETH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FARRO MANSILLA,  SARA MELISSA' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'FEBRES PORTAL,  NATHALIE PAMELA' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'FERNANDEZ ABAD,  RUTH ELIZABETH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FERNANDEZ AGREDA,  GIULIANA VICTORIA' Then 'GALLEGOS VALDEZ,  MARY ANN' " +
                                                            "When asesor = 'FERNANDEZ CASTILLO,  WENDY MARCELA' Then 'ROJAS,  ZAIDVICT' " +
                                                            "When asesor = 'FERRE VELASQUEZ,  ELIZABETH KATERINE' Then 'TOVAR, PATRICIA' " +
                                                            "When asesor = 'FERRO SEMINARIO,  PAOLA MARIELLA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FICEFA S.A.C' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'FIESTAS VALLADARES,  CECILIA GENARA' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'FIGUEROA MANRIQUEZ, ANDREA STEFANIA' Then 'IZAGUIRRE, ROXANA' " +
                                                            "When asesor = 'FLORES BENDEZU,  CLAUDIA MARILYN' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'FLORES FLORES,  SEGUNDO' Then 'SALAS, FARLYN' " +
                                                            "When asesor = 'FLORES TORRES,  GIANCARLO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FLORES VASQUEZ, MIRYAM YANET' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FLORES VIGO,  KELLY JUDITH' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'FLORINDEZ SEGURA,  GLORIA ISABEL' Then 'RENGIFO RENGIFO,NINI ISABEL' " +
                                                            "When asesor = 'FOSCA GAMBETTA,  STEFANO GIUSEPPE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FRANCO RAFAEL,  ROSA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FREYRE SOLARI,  CECILIA HILDA' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'FRIAS BILLINGHURST,  LIZ MARICELA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'FRIAS CRUZ,  JUAN CARLOS' Then 'DELGADO, CLAUDIA' " +
                                                            "When asesor = 'FUENTES PEÑA,  MARIA ADRIANA DEL ROSARIO' Then 'MORAN MAURICIO DE LOYOLA,  MAGDALENA MARIA' " +
                                                            "When asesor = 'FYF DARUICH CORREDORES DE SEGUROS SAC' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'GAGO SILVA,  MARIA ELENA' Then 'NAVARRETE, DORA' " +
                                                            "When asesor = 'GALARZA POMASUNCO,  YESSICA CLAUDIA' Then 'FELIPA, SUMIKO YASBEL' " +
                                                            "When asesor = 'GALLARDO SUSANO,  SHARON CLARA' Then 'LLERENA, MARCO' " +
                                                            "When asesor = 'GALLEGOS VALDEZ,  MARY ANN' Then 'GALLEGOS VALDEZ,  MARY ANN' " +
                                                            "When asesor = 'GALVEZ TORRES,  ROSALIA' Then 'SILVA CHAVEZ,MARCELA DEL ROSARIO' " +
                                                            "When asesor = 'GALVEZ VALERA,  MARGARITA' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'GAMALLO BALCAZAR DE REYNA,  DANIELLA' Then 'GOMEZ, JESSICA' " +
                                                            "When asesor = 'GAMARRA FUSTAMANTE,  JORGE GORIK' Then 'TOVAR, PATRICIA' " +
                                                            "When asesor = 'GAMARRA LUJAN,  MAYCOL JESUS VICTOR' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'GAMARRA VERGARA,  CECILIA ADELA' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'GAMIO ALBURQUEQUE,  CLAUDIA ZARELA' Then 'DELGADO, CLAUDIA' " +
                                                            "When asesor = 'GARATE MONDRAGON,  KARLA JACKELINE' Then 'MARQUEZ,  LESLY' " +
                                                            "When asesor = 'GARCIA CASTILLO,  DELLY' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GARCIA CUMPA,  ALMA MAGDALENA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GARCIA DELGADO,  ROUTH ELITH' Then 'DIAZ GARCIA,  ELTON JHON' " +
                                                            "When asesor = 'GARCIA ORMEÑO, NELLA VICTORIA' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'GARCIA SOTO,  VICTOR LUIS' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'GARCIA VALLADARES,  MARIA GUISELA' Then 'PISCOYA, SONIA' " +
                                                            "When asesor = 'GARCIA ZAPATA,  ANIA ISABELLA' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'GAYOSO NUÑEZ,  ANUSKA ZUSSETTI' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'GIL CAMPOS,  ANA PATRICIA' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'GIL ZEVALLOS,  CARLA GABRIELA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GOMERO CESPEDES DE GOMEZ,ELBIA NANCY' Then 'PURIZAGA RAMOS,FANNY ELENA' " +
                                                            "When asesor = 'GOMES ROJAS,  KAREN ROSSMERY' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GOMEZ ARROYO,  KATIUSKA PAOLA' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'GOMEZ CAVERO,  JUAN GUILLERMO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GOMEZ GUZMAN,  CLAUDIA ALEJANDRA' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'GONZALES CRUZ,  YURICO YUVISSA NATHALY' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'GONZALES DIAZ,  MARIBEL' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'GONZALES GARCIA,  ROCIO ESMERALDA' Then 'INFANTE, ERNESTO' " +
                                                            "When asesor = 'GONZALES MELGAR,  JACQUELYN LORENS' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GONZALES PAUCAR,  JACKELINE YAMALI' Then 'PERALTA, DANIELA' " +
                                                            "When asesor = 'GONZALES ZUÑIGA,  ANISA RACHIDA' Then 'SALAS, FARLYN' " +
                                                            "When asesor = 'GONZALES ZUÑIGA,  MARIELLA EMILIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GOVEA LOPEZ DE DE LA BARRERA,LAURA ' Then 'PYE SOLARI DE GONZALEZ,  JANE LOUISE' " +
                                                            "When asesor = 'GOYBURO ORTIZ,  JENNEIFER JACQUELYN' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'GRADOS VILCAPOMA,  ROSARIO SOLEDAD' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'GRANDY ALCALDE,  LISETTE MILAGROS' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GRIMALDI PELTROCHE,  PERCY' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'GUERRERO LEGUA,  OSCAR ORLANDO' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'GUERRERO URPEQUE,  ERICK FABIAN' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'GUEVARA ALPACA,  JOSHUA ANDRE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GUEVARA MENDOZA,  MERY ISABEL' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'GUILLEN IBARCENA,  SHIRLEY GIANNINA' Then 'CUADROS,  PAOLA' " +
                                                            "When asesor = 'GUILLEN MENDOZA,  LAURA PATRICIA' Then 'PACHECO, ZAIDA' " +
                                                            "When asesor = 'GUILLEN NEYRA,  ROSA KARINA' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'GUTIERREZ ALVARADO,ANGELINA MAGDALENA' Then 'ROMERO ALCEDO,GINA ORFELINDA' " +
                                                            "When asesor = 'GUTIERREZ ARTEAGA,  LUZ MILAGROS' Then 'ROJAS,  ZAIDVICT' " +
                                                            "When asesor = 'GUTIERREZ MARROQUIN,  ANA ESTEFANI' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GUTIERREZ MEDRANO,  ROXANA IRIS' Then 'TRIGOSO, YISEL' " +
                                                            "When asesor = 'GUZMAN PEREZ DE LATURE,  ROSA KARINA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'GUZMAN PEREZ DE LATURE, ROSA KARINA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'HANCO GARCIA,  DAYANN LIZETH' Then 'CUADROS,  PAOLA' " +
                                                            "When asesor = 'HERMOZA VINCES, GONZALO FERNANDO' Then 'COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'HERNANDEZ MORENO,  GRACIELA EUGENIA' Then 'HERNANDEZ MORENO,  GRACIELA EUGENIA' " +
                                                            "When asesor = 'HERRERA RIOS DE VEGA,ELVA ROSA' Then 'COORDINACION LIBRE, SAN BORJA (1)' " +
                                                            "When asesor = 'HOLGUIN CRUZ,  GIOVANA DEL CARMEN' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'HUAMAN ARBULU,  GIULLIANA PATRICIA DEL CARMEN' Then 'SALAS, FARLYN' " +
                                                            "When asesor = 'HUAMAN FARFAN,  NURY MARCIA' Then 'CIEZA, LILIANA' " +
                                                            "When asesor = 'HUAMANCHUMO RAMIREZ,  MARIA ANGELICA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'HUANATICO PINTO,  NORMA' Then 'CUADROS,  PAOLA' " +
                                                            "When asesor = 'HUERTAS MOGOLLON,  HOLDAVID AURELIO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'HURTADO TEMBLADERA,  JANET MAYELA' Then 'GONZALES,  MICHELANGELO' " +
                                                            "When asesor = 'HURTADO VALDIVIA,  BRAYAN PAUL' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'INECV E.I.R.L' Then 'RAMIREZ RODRIGUEZ,MARISA ROSARIO' " +
                                                            "When asesor = 'INFANTE PRIAS,  LORENA PAOLA' Then 'EGOAVIL, JOEL' " +
                                                            "When asesor = 'IRUJO DEL AGUILA,  MILUSKA ANTONIETA' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'ITAL SEGUROS SA CORREDORES DE SEGUROS' Then 'COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'JIMENEZ MERINO, LINDSAY KATHERINE' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'JULCA OLANO,  MACYORI MISHELL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'JULCAHUANCA JIMENEZ,  ANITA ELVIRA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'KAM LEON,  CINTHYA BEATRIZ' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'KONECTA DIGITAL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LA POSITIVA SEGUROS Y REASEGUROS S.A.A. - BANCO PICHINCHA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LA PROTECTORA CORREDORES DE SEGUROS S.A.' Then 'COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'LA ROSA ALVAREZ,  FELIX FERNANDO' Then 'FELIPA, SUMIKO YASBEL' " +
                                                            "When asesor = 'LADINES SAAVEDRA,  MARITZA ESTHER' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LANDAVERI GRIMALDO,  JOSE ALEJANDRO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LAVERIAN OTERO,  NELIDA VANESSA' Then 'CIEZA, LILIANA' " +
                                                            "When asesor = 'LAZO MERINO,  LUIS GUSTAVO' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'LEGOAS ORDOÑEZ,  NELSON KLISMAN' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LEIVA PALACIOS,  MERCEDES ROSARIO' Then 'FELIPA, SUMIKO YASBEL' " +
                                                            "When asesor = 'LEON DINCLANG,  MARGARITA MANUELA' Then 'NOVOA, LLANDER' " +
                                                            "When asesor = 'LEON MEDINA,  ELIZABETH YURI' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'LEON OBALLE,  MARIA VERONICA' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'LINARES VILLACORTA,  CINTHYA ISABELA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LIZARDO PEREZ,  SAGRARIO DE LOS ANGELES' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LLERENA VARGAS,  MARCO ALONSO' Then 'LLERENA VARGAS,  MARCO ALONSO' " +
                                                            "When asesor = 'LLOSA GAMARRA,  JHOAN LISSETH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LLUEN GASTELO,  VANESSA ELIZABETH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LLUNCOR MAYANGA,  DIEGO ARNALDO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LOCHER PRECIADO,  FRANCESCA' Then 'TOVAR, PATRICIA' " +
                                                            "When asesor = 'LOPEZ CASTILLO,  VERONICA ELIZABETH' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'LOPEZ FARRO,  GISELA ELIZABETH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LOPEZ GAMONET,  JOHANNA ALEXANDRA' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'LOPEZ GUZMAN,  HELEN YULMAR' Then 'MERINO, FRANK' " +
                                                            "When asesor = 'LOPEZ MARCILLA,  MARIA TERESA' Then 'PERALTA, DANIELA' " +
                                                            "When asesor = 'LOPEZ PEÑA,  KERLY EVELYN' Then 'NAVARRETE, DORA' " +
                                                            "When asesor = 'LOPEZ VALDEZ,  LUIS ENRIQUE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LOZADA VIERA,  CARMEN JULIA' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'LUCCHINI LAZARO,  FERNANDO JOALDO' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'LUJAN CAVERO,  CLAUDIA NOEMI' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'LUNA BARRAZA,  SANDRA MARIELA' Then 'BUITRON LOLY,  CARMEN ROSARIO' " +
                                                            "When asesor = 'MACARLUPU TORRES,  ANA CINTHIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MACEDO SAIRITUPA,  JESSICA ROSMERY' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MADRID RUMICHE,  ROSA SOFIA' Then 'RENGIFO RENGIFO,NINI ISABEL' " +
                                                            "When asesor = 'MALCA ESPINOZA,  GLORIA INES' Then 'CASTRAT FIEGE,  MARIA DEL PILAR' " +
                                                            "When asesor = 'MAMANI MAR, MARIA ISABEL' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'MANCHEGO NUÑEZ ROBLES DE BALLON,  CARMEN LOURDES' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MANRIQUE NAVARRO,  JUANA JAQUELINE' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'MANTILLA MEZA,  ALAN GABRIEL' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'MANZO SANTOS,  EVA DINA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MAQUERA GAMBETTA,  ARACELI YAMILET' Then 'PACHECO, ZAIDA' " +
                                                            "When asesor = 'MARCELO PEREZ DE DELGADO,  MERCEDES JULISSA' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'MARES CARRILLO,  ALDANA PEGGY' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'MARIN FLORES,  SANDRA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MARQUEZ ESPINOZA,  ROXANA BEATRIZ' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MARRO CABEZAS,  CARMEN MARTHA' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'MARSH REHDER S.A.C. CORREDORES DE SEGUROS' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'MARTINEZ ENCALADA,  CECILIA FRANCISCA' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'MASCARO VENTO,  LUCY VICTORIA' Then 'NOVOA, LLANDER' " +
                                                            "When asesor = 'MASSA GARCIA,  MARTHA ELENA' Then 'CABRERA MATUTE,  NIDIA SORAYA' " +
                                                            "When asesor = 'MATHEUS AGUIRRE, GABRIELA LUCILA' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'MATIENZO RAMIREZ,  MAYRA CINTHIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MAURICIO BACA,  ROCIO VIVIANA' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'MAURICIO GARRIDO,  SANDRA MARINA' Then 'NIMA, SILVIA' " +
                                                            "When asesor = 'MAUTINO CARBAJAL,  GIANNINA MIA' Then 'COORDINACION LIBRE, SAN BORJA (1)' " +
                                                            "When asesor = 'MAVAC CORREDORES DE SEGUROS S.A.C.' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'MAYORGA MEZA,  SANDRA MILAGROS' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'MAYTA MARROQUIN DE ESCUDERO,  MARIA EUGENIA' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'MAZA ROMERO,  TATIANA MARJHORET' Then 'TOVAR, PATRICIA' " +
                                                            "When asesor = 'MAZULIS CHAVEZ,  CAROLINA GYSLEIN' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'MAZZEI HOLGUIN,  VANESSA ILIANA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MEDINA ELGUERA,  PAMELA ROSARIO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MEDINA MONAR, MARLENE JOSEFA' Then 'COORDINACION LIBRE, SAN BORJA (1)' " +
                                                            "When asesor = 'MEDINA QUIROZ,  INES CAROLINA' Then 'COORDINACION LIBRE, SAN BORJA (1)' " +
                                                            "When asesor = 'MEDINA VIVANCO DE LAGUNA,  PATRICIA IVONNE' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'MEIGGS MENDOZA,  BRUNO ANTONIO MARTIN' Then 'CIEZA, LILIANA' " +
                                                            "When asesor = 'MELENDEZ MIRANDA,  JHON FRANKLIN' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MELLY USQUIANO DE SAMAME,  CATALINA ISABEL' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'MENDEZ CANALES,  VIRGINIA DORA' Then 'CASTRAT FIEGE,  MARIA DEL PILAR' " +
                                                            "When asesor = 'MENDEZ FAJARDO,  CLAUDIA FIORELLA' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'MENDOZA ACOSTA,PATRICIA LILY' Then 'COORDINACION LIBRE, SAN BORJA (1)' " +
                                                            "When asesor = 'MENDOZA DE LA PEÑA,  KATHERINE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MENDOZA HEREDIA,  MURYELL ARIANE' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'MENDOZA ORTIZ,  JIMMY SMITH' Then 'ORTIZ, MARIBEL' " +
                                                            "When asesor = 'MENDOZA RAMOS,  IVONNE MARCELA' Then 'PACHECO, ZAIDA' " +
                                                            "When asesor = 'MENDOZA SILVA,  KAREN ANGELICA' Then 'MOGOLLON, SANDRA' " +
                                                            "When asesor = 'MENDOZA VASQUEZ,  ANAI DEL SOL' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'MENDOZA VASQUEZ, ANAI DEL SOL' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'MERCADO PARIONA,  LYZ EVELYN' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MERINO CALERO,  MARY KELLY' Then 'LLERENA, MARCO' " +
                                                            "When asesor = 'MESONES LINARES,  JULISSA ELIANA' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'MIÑAN ABAD,  JOHANA CAROLINA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MICHILOT HUERTAS, ADA GUISELLA' Then 'RAMIREZ RODRIGUEZ,MARISA ROSARIO' " +
                                                            "When asesor = 'MIRANDA CASTRO,  KATY NOELIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MOGOLLON CARMEN,  SANDRA MILUSKA' Then 'MOGOLLON CARMEN,  SANDRA MILUSKA' " +
                                                            "When asesor = 'MOGROVEJO ARCE DE AGUILAR,  RENATA ISELA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MONCADA GARCIA,  ANDREA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MONTALVAN BARRETO,  GLADYS CATHERINE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MONTENEGRO ROMAN,  MARIA ESTHER' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'MONTERO AGUILA,  MARIA FERNANDA' Then ', VACANTE PIURA' " +
                                                            "When asesor = 'MONTERO BERAMENDI,  MARIA DEL CARMEN' Then 'VENTURO, NADIA' " +
                                                            "When asesor = 'MONTERO QUINTANA,  FABIOLA MIRELLA' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'MONTES GONZALES DE GALINDO,SILVIA ANA' Then 'ANTICONA VELASQUEZ,  VANESSA KATHERINE' " +
                                                            "When asesor = 'MONTEZA GONZALES,  MAYRA YESSENIA' Then 'VENTURO, NADIA' " +
                                                            "When asesor = 'MORAN MAURICIO DE LOYOLA,  MAGDALENA MARIA' Then 'MORAN MAURICIO DE LOYOLA,  MAGDALENA MARIA' " +
                                                            "When asesor = 'MORAN TINOCO,  MERCY ESTANITA' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'MORENO MONTALVO,  LUZ TRINIDAD' Then 'TOVAR, PATRICIA' " +
                                                            "When asesor = 'MORENO RUIZ,  JOAO MANUEL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MORENO SOLORZANO,  YESSENIA JHETALLY YAJAIRY' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'MORENO TANTAVILCA,  ESTEYSI SILVIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MOSTACERO RODRIGUEZ,  FIORELLA DEL CARMEN' Then 'IZAGUIRRE, ROXANA' " +
                                                            "When asesor = 'MUCHA ARIAS,  PATRICIA ELIZABETH' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'MUCHA QUISPE,  MARLENE MARIBEL' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'MURILLO ALVA,  ADRIANA PATRICIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'MYASES E.I.R.L' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'NAKANDAKARI SHIMABUKURO,ANA ' Then 'ISHARA NAKASONE,JULIA BEATRIZ' " +
                                                            "When asesor = 'NAVARRO DELGADO,ELVIA DEL MILAGRO' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'NAVARRO NIQUIN,  JOHANNA BRENDA' Then 'INFANTE, ERNESTO' " +
                                                            "When asesor = 'NAVARRO REY SANCHEZ,  FIORELLA DEL CARMEN' Then 'VENTURO, NADIA' " +
                                                            "When asesor = 'NAZARIO CHAVEZ,  MARIA DEL CARMEN' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'NECIOSUP CHINCHAY,  OSCAR DANY' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'NEWELL MENESES,  WENDY PAMELA' Then 'GOMEZ, JESSICA' " +
                                                            "When asesor = 'NEWELL MENESES, WENDY PAMELA' Then 'GOMEZ, JESSICA' " +
                                                            "When asesor = 'NEYRA AREVALO,  ANITA CLAUDIA' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'NIEVES FALLA,  PIERINA' Then 'MOGOLLON, SANDRA' " +
                                                            "When asesor = 'NOÑUNCA CRISPIN,  ANGGEL LIZBETH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'NORIEGA SANCHEZ,  ROCIO DEL PILAR' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'NORIEGA TABORI VDA DE CAMPODONICO,ELISA LILIANA' Then 'SILVA CHAVEZ,MARCELA DEL ROSARIO' " +
                                                            "When asesor = 'NOVOA MENDOZA,  WILBERT MANUEL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'NUÑEZ DEL PRADO REYNOSO,  JUAN JESUS' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'NUÑEZ GUZMAN,  MARIA SOLEDAD' Then 'LLERENA, MARCO' " +
                                                            "When asesor = 'OBANDO FLORES,  DEYSI RUBI' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'OBISPO CASTRO,  JULISSA KATHLEEN' Then 'PERALTA, DANIELA' " +
                                                            "When asesor = 'OCHANTE BERNARDINO,  JESSICA' Then 'FELIPA, SUMIKO YASBEL' " +
                                                            "When asesor = 'OJEDA QUIROZ,  FELIPE MOISES' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'OLIVARES DENEGRI,  JORGE LUIS' Then 'NOVOA, LLANDER' " +
                                                            "When asesor = 'OLIVARES INFANTES,  MARCO ANTONIO' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'OLORTIGA CUADRADO,  CINTYA SOLEDAD' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ORE TORRES,  DAYANNA JAZMIN' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'ORMEÑO PANTA,  ORIANA STEPHANIE' Then 'CIEZA, LILIANA' " +
                                                            "When asesor = 'OROZCO JIMENEZ,  ROCIO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ORTIZ VELASCO,  MARIA JANICE' Then 'GRANDA, ROY' " +
                                                            "When asesor = 'OSORIO VALDEZ,  MARIA ROXANA' Then 'VENTURO, NADIA' " +
                                                            "When asesor = 'OSSIO PORRAS,  LOURDES PELAYA' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'OYARCE LUDEÑA,  BRUNELLA ANTOANNE' Then 'MARQUEZ,  LESLY' " +
                                                            "When asesor = 'PACHECO BADOINO,  MARIANELLA GUISEPPINA' Then 'CUADROS,  PAOLA' " +
                                                            "When asesor = 'PACORA VASQUEZ,  MILAGROS' Then 'MOGOLLON, SANDRA' " +
                                                            "When asesor = 'PADILLA BRAVO,  JESSICA PAMELA ROSARIO' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'PADILLA USQUIANO,  JUDITH ROSARIO' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'PAIVA LA ROSA,  SARA JANNET' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'PALACIOS HERRERA, OLGA ANGELICA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'PALAS TAPIA,  MARIA ELENA DE LOS MILAGROS' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'PALOMINO CACERES,  LUZ ELENA' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'PALOMINO RAMIREZ, MARLENY' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'PANTA DIAZ,  JACKSON MARTIN' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'PANTA VELEZMORO,  DIANA JATSUMI' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'PANTIGOSO LEON,  JHASHAIRA MILAGROS' Then 'LLERENA, MARCO' " +
                                                            "When asesor = 'PARKER RENGIFO,  LAURA MONICA' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'PASARA VELASQUEZ, CARMEN CLOTILDE' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'PASARA VELASQUEZ,CARMEN CLOTILDE' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'PASTOR FLORES,  VIVIANA ISABEL' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'PAUCAR RAMIREZ,  CAROL KATHERINE' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'PEÑA MELENDRES,  GIANFAVIO JOSHUA' Then 'MERINO, FRANK' " +
                                                            "When asesor = 'PEÑA PAJARES,  GISELA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'PELLA ADRIANZEN,  INGRID MILAGROS' Then 'GRANIZO, ALEXANDRA' " +
                                                            "When asesor = 'PENNY BIDEGARAY,  RICHARD JUAN' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'PENNY BIDEGARAY,  RICHARD JUÁN JOSÉ DE LA CRÚZ' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'PERALTA OLIVARES,  YRINA PAOLA' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'PERALTA WALTER, MONICA ALEJANDRA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'PEREYRA DE MORALES,  OLINDA FIDELIA' Then 'PEREYRA DE MORALES,  OLINDA FIDELIA' " +
                                                            "When asesor = 'PEREZ BECERRA,  JULIO JEISER' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'PEREZ BEDOYA,  LETICIA MONICA' Then 'CUADROS,  PAOLA' " +
                                                            "When asesor = 'PEREZ HUAMAN,  IRMA MARIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'PEREZ OSORIO,  ELSA MERCEDES' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'PEREZ WALDE,  ADRIANA ANNABELL' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'PINADO LOPEZ DE FACUNDO,  EVELYN GERALDINE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'PINADO LOPEZ,  EVELYN GERALDINE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'PINEDO REATEGUI,CLEDY LUZMILA' Then 'COORDINACION LIBRE, SAN BORJA (1)' " +
                                                            "When asesor = 'PINILLOS RUSSO,  GIULIANA TERESA' Then 'CAJUSOL CHEPE,ROSA NEGDA' " +
                                                            "When asesor = 'PINTO BENITES,  ROXANA MARGOTH' Then 'GRANDA, ROY' " +
                                                            "When asesor = 'PINTO BOHORQUEZ, MIRELLA ' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'PINTO PEÑA,  ANDRE ADEMIR ALFREDO' Then 'ORTIZ, MARIBEL' " +
                                                            "When asesor = 'PISCOYA VENTURA,  MARIA MAGDALENA DE JESUS' Then 'GOMEZ, JESSICA' " +
                                                            "When asesor = 'PLAZA VIDAURRE,  HILDA TERESA JACINTA' Then 'PYE SOLARI DE GONZALEZ,  JANE LOUISE' " +
                                                            "When asesor = 'POLO REAÑO,  CARLOS MANUEL' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'POMA PALOMINO,  ROLANDO JAMES' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'POMA PEREZ DE MONTALVO,  KATHIE ISABEL' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'POMEZ TICONA,  MIGUEL ANGEL' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'PONCE CACEDA,  DIANA ELIZABETH' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'PORTALES JARA,  ERICK JAVIER' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'PRATO NARANJO,  GABRIELA ELENA TINA' Then 'SILVA CHAVEZ,MARCELA DEL ROSARIO' " +
                                                            "When asesor = 'PREVESALUD E.I.R.L.' Then 'MERLUZZI, VALLADARES MIRELLA' " +
                                                            "When asesor = 'PRIETO HERRERA,  ANYIE EDITH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'PROLIFE COMERCIALES SAC' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'PULGAR CASTILLO,ELIZABETH GRACIELA' Then 'HERNANDEZ MORENO,  GRACIELA EUGENIA' " +
                                                            "When asesor = 'PUMA CALLE,  ANGELICA MURIELL' Then 'MARQUEZ,  LESLY' " +
                                                            "When asesor = 'PUPUCHE GAMARRA,  YECENIA PAOLA' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'PYE SOLARI DE GONZALEZ,  JANE LOUISE' Then 'PYE SOLARI DE GONZALEZ,  JANE LOUISE' " +
                                                            "When asesor = 'QUEZADA OLIVEIRA,  NOELIA NORMA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'QUIÑONES CASTILLO,  JESUS FERNANDO' Then 'GRANDA, ROY' " +
                                                            "When asesor = 'QUIÑONES  PARIONA,  LUIS JERSSI' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'QUINTANA GUEVARA,  GISELLA ELENA' Then 'NOVOA, LLANDER' " +
                                                            "When asesor = 'QUIROZ RIOS,  GABRIELA ELIZABETH' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'QUIROZ RIOS, GABRIELA ELIZABETH' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'QUISPE POZO,  SOLENA GISELLE MERCEDES' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'RABANAL CONTRERAS,  ALICIA JESUS' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'RAMIREZ AÑAZGO,  ERIKA VANESSA ADALIA' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'RAMIREZ QUIJANDRIA,  ADRIANA PATRICIA' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'RAMIREZ QUIJANDRIA, ADRIANA PATRICIA' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'RAMIREZ RODRIGUEZ,  MARISA ROSARIO' Then 'ABAD LUNA, IVAN' " +
                                                            "When asesor = 'RAMIREZ SALAZAR,  CINTHYA HELENA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'RAMOS CAYO,  SILVIA ROSSANA' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'RAMOS DIAZ,  CECILIA' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'RAMOS LAOS,  TANIA MELISSA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'RAMOS LOZANO,  CARLOS EDUARDO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'RAMOS RUIZ,  SARA GUISELA' Then 'PISCOYA, SONIA' " +
                                                            "When asesor = 'RAMOS SAENZ,  ORLANDO DANIEL' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'RAMOS VASQUEZ,  ERIN OMAR' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'RAMOS VASQUEZ, ERIN OMAR' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'RAZURI GONZALES,  ANDREA YSABEL' Then 'TOVAR, PATRICIA' " +
                                                            "When asesor = 'REAÑO VELASQUEZ,  MIRTHA ELIZABETH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'REATEGUI PEREZ,  KARLA PAOLA' Then 'NIMA, SILVIA' " +
                                                            "When asesor = 'REINOSO PACHECO DE CONTRERAS,  JACQUELINE SUSANNE' Then 'BROKERS COMPAщAS, asesorAS' " +
                                                            "When asesor = 'REQUEJO YALTA,  CAROLINA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'REVOREDO LLANOS, MARIA ELIZABETH' Then 'ANTICONA VELASQUEZ,  VANESSA KATHERINE' " +
                                                            "When asesor = 'REYES BALAREZO,  NAHOMI DE LA PAZ' Then 'GOMEZ, JESSICA' " +
                                                            "When asesor = 'REYES CALLE,  VALERIA AURORA' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'REYES GUTIERREZ,  MARIA PATRICIA' Then 'PACHECO, ZAIDA' " +
                                                            "When asesor = 'REYES OCAMPO,  TERESA VICTORIA' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'REYES UGAZ,  ANGELA LUCIA' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'RIOS PALOMINO,  ERICA MELINA' Then 'IZAGUIRRE, ROXANA' " +
                                                            "When asesor = 'RIOS QUIROZ,  SARA CELIA' Then 'VENTURO, NADIA' " +
                                                            "When asesor = 'RIVAS SERNAQUE,  MONICA' Then 'MOGOLLON, SANDRA' " +
                                                            "When asesor = 'RIVASPLATA MALCA,  GIOVANA AYDEE' Then 'EGOAVIL, JOEL' " +
                                                            "When asesor = 'RIVERA MOGOLLON,  ILIANA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'RIVERA RIVERA,  GIOVANA ISABEL PATRICIA' Then 'PACHECO, ZAIDA' " +
                                                            "When asesor = 'RIVERA SILVA,  JUAN DIEGO' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'RIVERA VASQUEZ,  INES STEFANY' Then 'ORTIZ, MARIBEL' " +
                                                            "When asesor = 'RIVERA VILELA,  MILAGROS DEL CARMEN' Then 'PISCOYA, SONIA' " +
                                                            "When asesor = 'RIVERA VILELA, MILAGROS DEL CARMEN' Then 'PISCOYA, SONIA' " +
                                                            "When asesor = 'ROBLES GABANCHO DE CARRASCO,  YISELA CARMELA' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'ROBLES NORIEGA,  ZOILA ESPERANZA' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'RODRIGUEZ GAMARRA,  SONIA ESTHER' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'RODRIGUEZ GARCIA,  MONICA CLOTILDE' Then 'CAJUSOL CHEPE,ROSA NEGDA' " +
                                                            "When asesor = 'RODRIGUEZ GOMEZ,  JUAN MIGUEL' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'RODRIGUEZ RIVERA,  GLADYS ELIZABETH' Then 'MARQUEZ,  LESLY' " +
                                                            "When asesor = 'RODRIGUEZ SANCHEZ DE SALVATIERRA, BEATRIZ D FIORELLA' Then 'Sin Coordinador' " +
                                                            "When asesor = 'RODRIGUEZ UBILLUS,  RAFAELA LORENA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ROJAS ALCOCER,  NERY JUANA' Then 'CABRERA MATUTE,  NIDIA SORAYA' " +
                                                            "When asesor = 'ROJAS ALVA,  RUBEN RUBEN' Then 'ROJAS ALVA,  RUBEN RUBEN' " +
                                                            "When asesor = 'ROJAS CORDOVA,  VILMA LOURDES' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'ROJAS DIAZ,  CAROLINA ELIZABETH' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'ROJAS HUARINGA,  ANALY MILAGROS' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'ROJAS RIOS,  ANA MARIA' Then 'TRIGOSO, YISEL' " +
                                                            "When asesor = 'ROMACO S.A.C. CORREDORES DE SEGURO' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'ROMERO CHAMORRO,  LIZET DOMENICA' Then 'AGURTO,  FRANCIA LUIS' " +
                                                            "When asesor = 'RONCALLA LUQUE,  CAMILA DEL CARMEN' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'RONDON ARCE,  JONATHAN FRANK' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'ROSAS OLIVEROS,  ZARELLA LIZBETH' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'RUIZ BARDALES,  ELSA JUDITH' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'RUIZ CUADROS,  CAROL VANESSA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'RUIZ MERA,  STEPHANIE MARICRUZ' Then 'GOMEZ, JESSICA' " +
                                                            "When asesor = 'RUMICHE LAZO,  ERICK JOE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SAAVEDRA DOMINGUEZ,  ROCIO NEILYTH' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'SAAVEDRA RUIZ,VILMA YOLANDA' Then 'ROMERO ALCEDO,GINA ORFELINDA' " +
                                                            "When asesor = 'SAENZ CALZADO,  RICHARD WILLIAMS' Then 'GRANDA, ROY' " +
                                                            "When asesor = 'SALAS MARTINEZ,  LUISA CAROLINA' Then 'EGOAVIL, JOEL' " +
                                                            "When asesor = 'SALAS MONTOYA,  MARIA EUGENIA JACQUELINE' Then 'ARENAS ZEBALLOS,  YBANA LUZ MARINA' " +
                                                            "When asesor = 'SALAS ROMERO,MARY CAROL' Then 'PURIZAGA RAMOS,FANNY ELENA' " +
                                                            "When asesor = 'SALAS VELAZCO,  ELIANA SILVIA' Then 'CONCHA, FABIOLA' " +
                                                            "When asesor = 'SALAZAR GUERRERO,  LENIZ LUPITA' Then 'CIEZA, LILIANA' " +
                                                            "When asesor = 'SALAZAR GUEVARA,  SOLDVER CLARK' Then 'MERINO, FRANK' " +
                                                            "When asesor = 'SALAZAR PARRA,  CARLOS ALFREDO' Then 'NOVOA, LLANDER' " +
                                                            "When asesor = 'SALAZAR VARGAS DE TOMATIS,  LILIANA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SALINAS HUATANGARE,  JUANA LUCILA' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'SALINAS PAREDES,  ROSA EDITH' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'SALINAS TODOROVICH,  ALEJANDRO XAVIER' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'SALIRROSAS ESPEJO,  ANA MARIA' Then 'HERNANDEZ MORENO,  GRACIELA EUGENIA' " +
                                                            "When asesor = 'SALVATIERRA MENDOZA,  ANA MARIA' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'SANCHEZ BARBOZA,  YEISY SEBASTIANA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SANCHEZ CARRANZA,  DANILO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SANCHEZ CASTAÑEDA,  YANINA ANABEL' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'SANCHEZ CHAVEZ,  ROSARIO DE LAS MERCEDES' Then 'EGOAVIL, JOEL' " +
                                                            "When asesor = 'SANCHEZ LLENQUE,  KARLA ISABEL' Then 'EGOAVIL, JOEL' " +
                                                            "When asesor = 'SANCHEZ POLO,  YSELA JANET' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SANCHEZ POSSO,  GERSON EUSEBIO' Then 'MARQUEZ,  LESLY' " +
                                                            "When asesor = 'SÁNCHEZ RODRIGUEZ,  RUDI ESTEFANI' Then 'GRANDA, ROY' " +
                                                            "When asesor = 'SANCHEZ RODULFO,  JENIFER ROSALYN' Then 'NOVOA, LLANDER' " +
                                                            "When asesor = 'SANCHEZ SOJO, SYBIL MARIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SANDOVAL FLORES,  PEDRO WILBERTO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SANGAMA WEILL,  JACKELINE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SANTA CRUZ ALEJANDRIA,  ANATALY' Then 'GARCIA, SHIRLEY' " +
                                                            "When asesor = 'SANTA CRUZ CENTURION,  ESTEPHANY TERESA' Then 'TOVAR, PATRICIA' " +
                                                            "When asesor = 'SANTA CRUZ POLUCHE,  RONAL GUSTAVO' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'SANTISTEBAN GONZALES,  JULIO CESAR' Then 'MOGOLLON, SANDRA' " +
                                                            "When asesor = 'SANTTI QUEREVALU,  MARIA DE LOURDES' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'SARMIENTO ACUÑA,  LADY MARYURI MARTINA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SARMIENTO MENDOZA,  MARGARITA MILAGROS' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SCC-ELITE VIRTUAL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SCHULZ CACERES,  KAREM DEL SOCORRO GENOVEVA' Then 'PEREYRA DE MORALES,  OLINDA FIDELIA' " +
                                                            "When asesor = 'SEGOVIA HUAMBACHANO,  MARIA ROSA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SEGURA SONO,  IRENE DEL ROCIO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SEJO SUAREZ,  LILIANA CRISTINA' Then 'FELIPA, SUMIKO YASBEL' " +
                                                            "When asesor = 'SEVILLA YATACO,  SILVIA' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'SIANCAS VIERA,  SOCORRO NOEMI' Then 'NAVARRETE, DORA' " +
                                                            "When asesor = 'SIFUENTES OLAECHEA CORREDORES DE SEGUROS S.A.C.' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'SIME CORREA,  HECTOR SANTIAGO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SIRHUA BOLO,  MICHELLE' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'SIU IGLESIAS,DORA ' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'SOLANO LATORRE,  EMANUEL ERNESTO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SOLIS GARCIA,  EVELYN JASMILETH' Then 'MARQUEZ,  LESLY' " +
                                                            "When asesor = 'SOLIS MORALES,  EVA LUCERO' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'SOSA ARBAIZA,  CYNTHIA VANESSA' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'SOSA PANTA, LUIS REYNALDO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SOTO QUIJANO,  CECILIA TERESA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'SUCLUPE CORONEL,  LUIS MANUEL' Then 'MERINO, FRANK' " +
                                                            "When asesor = 'SUCUPLE SANTISTEBAN,  JOSE ALEX' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'TAKAMURA VILLAR,  PRISCILLA' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'TALLEDO PILCO,  MARISELA BESSY' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'TAPIA ANDRES,  JUAN CARLOS' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'TAPIA MAICELO,  ANA DEISSY' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'TAVERA GUTIERREZ,  ZOILA IVETTE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'TEAS SOLARI,  GIULIANA MARISA' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'TELLO GONZALES,  ZULEMA ENRIQUETA' Then 'CIEZA, LILIANA' " +
                                                            "When asesor = 'TELLO GUEVARA,  MONICA MILAGROS' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'TELLO VARGAS,  JESSICA CATHERINA' Then 'Sin Coordinador' " +
                                                            "When asesor = 'TERAN CRUZ,  MARIA LAURA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'TERRONES CAMPOS,  JOSE LUIS' Then 'CABALLERO, HUMBERTO' " +
                                                            "When asesor = 'THOMAS MIRANDA,  SUE ELLEN' Then 'EGOAVIL, JOEL' " +
                                                            "When asesor = 'TICONA CASTILLO,  EVA ANGELICA' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'TITANIO CORREDORES DE SEGUROS S.A.C.' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'TOLEDO ZAPANA DE CAMPOS,  ELIZABETH VICENTA' Then 'CARBAJAL LIZARRAGA,MONICA DEL SOCORRO' " +
                                                            "When asesor = 'TORALES PEREA,  JAVIER GONZALO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'TORI POICON,  ZULEIKA LOURDES' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'TORO SOTOMAYOR,  CARMEN CECILIA' Then 'RENGIFO RENGIFO,NINI ISABEL' " +
                                                            "When asesor = 'TORPOCO MOYA,  MARGOT' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'TORRES AGURTO,  ROXANA MERCEDES' Then 'FARFAN, EMILY' " +
                                                            "When asesor = 'TORRES DELGADO,  TALIA IBET' Then 'GOMEZ, JESSICA' " +
                                                            "When asesor = 'TORRES FANOLA,  GINA VANESSA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'TORRES FARFAN,  ROSARIO HAYDEE' Then 'AREVALO, ANA' " +
                                                            "When asesor = 'TORRES FRANCO,  LIDIA ELIZABETH' Then 'GALLEGOS VALDEZ,  MARY ANN' " +
                                                            "When asesor = 'TORRES GARCIA,  ROSA LIZBELL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'TORRES KOC,  HELEN LILIANA' Then 'EGOAVIL, JOEL' " +
                                                            "When asesor = 'TORRES LABAN DE RENTERIA,  JYNNEE MARISOL' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'TORRES MENDOZA,  HORTENCIA MARIA' Then 'MORAN MAURICIO DE LOYOLA,  MAGDALENA MARIA' " +
                                                            "When asesor = 'TORRES MILLAN,  NAILETH NAZARETH' Then 'EGOAVIL, JOEL' " +
                                                            "When asesor = 'TORRES REATEGUI,  JACQUELINE LESLY' Then 'CIEZA, LILIANA' " +
                                                            "When asesor = 'TORRES TELLO,  CINTHIA KATHERYNE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'TORRES VILCA,  DENIS JOAQUIN' Then 'IZAGUIRRE, ROXANA' " +
                                                            "When asesor = 'TUCCIO VALVERDE,  JOSE ANTONIO' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'TUESTA ABENSUR,  RINA PAOLA VANESSA' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'UÑURUCO PANTI,  ALICIA' Then 'FELIPA, SUMIKO YASBEL' " +
                                                            "When asesor = 'UBIDIA MUNIVE,  ELISA RUTH' Then 'COORDINACION LIBRE, SAN BORJA (1)' " +
                                                            "When asesor = 'UGAZ BASTANTE,  NICOLL NISI' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'URBINA GUARNIZO, TUCKY ASTERIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'URBIOLA AYQUIPA,  ROSA CONSUELO' Then 'FELIPA, SUMIKO YASBEL' " +
                                                            "When asesor = 'URBISAGASTEGUI VARGAS,  CAROL JAZMIN' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'URDANIGA QUIJANDRIA,  ANGELICA JACKELINE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'UREÑA VILLAZANTE,  NEIL GREGORIO' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'URRUNAGA TORRES,  ROSA ADRIANA' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'URTEAGA PENEDO,  KATIA MERCEDES' Then 'GRANDA, ROY' " +
                                                            "When asesor = 'VALDERRAMA ELLIS,  ZERENITH ROSARIO' Then 'CUADROS,  PAOLA' " +
                                                            "When asesor = 'VALDIVIA CHACON,  ROSA MARLENE' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'VALDIVIEZO MOGOLLON,  ROSA GIULLIANA' Then 'PISCOYA, SONIA' " +
                                                            "When asesor = 'VALDIVIEZO NUÑEZ,  TANIA CLEOPATRA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'VALDIVIEZO NUÑEZ,  TERESA DEL PILAR' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'VALDIVIEZO ROBLEDO,  HELEN JENNIFER' Then 'BANEGAS, ISMAEL' " +
                                                            "When asesor = 'VALENCIA VASQUEZ,  KATTYUSCH MARIA LIBIA' Then 'VENTURO, NADIA' " +
                                                            "When asesor = 'VALENZUELA ACOSTA,  ROSA AMELIA EUCARIS' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'VALVERDE ARANA,  CRISTIAN ALEXANDER' Then 'FLORES,  JONATHAN' " +
                                                            "When asesor = 'VARELA DIAZ,  DONNA PATRICIA CECILIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'VASQUEZ LAZARTE,  SANDRA MARGOT' Then 'INFANTE, ERNESTO' " +
                                                            "When asesor = 'VASQUEZ LEON,  RENZO JHOVANNY' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'VASQUEZ PALOMINO,  LUIS MIGUEL YEMEN' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'VASQUEZ POMEZ,  CARMEN ISABEL' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'VEGA GAMARRA DE QUEIROLO,  NINA DEL PILAR' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'VEGA GAMARRA,  NINA DEL PILAR' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'VEGA NUÑEZ,  GLORY ANABELLA DE LOS MILAGROS' Then 'NIMA, SILVIA' " +
                                                            "When asesor = 'VELA FLORES,  MARIA GIULANNA' Then 'Sin Coordinador' " +
                                                            "When asesor = 'VELARDE CONSULTORES FINANCIEROS S.A.C.' Then 'SIN, SUPERVISION' " +
                                                            "When asesor = 'VELASQUE SAAVEDRA,  RENATO' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'VELEZ-VAUPEL & ASOCIADOS S.A' Then 'BROKERS COMPAÑIAS, asesorAS' " +
                                                            "When asesor = 'VELIZ PERLECHE,  NATALIA GRISELDA' Then 'DE LA PIEDRA, CARLA' " +
                                                            "When asesor = 'VENTOCILLA ZAVALA,  SARA ZULEMA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'VENTURA ARANGO,  SILVIA ISABEL' Then 'FELIPA, SUMIKO YASBEL' " +
                                                            "When asesor = 'VERA MENDOZA,  JUANA CECILIA' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'VERGARA ALARCON,  JUDITH BETTY' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'VERGARA GOMEZ,  GIOVANA GLORIA' Then 'ANAMARIA, NANCY' " +
                                                            "When asesor = 'VIDAL NAVARRETE,  JOSE LUIS' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'VIDALON ADRIANZEN,  DEBORAH MARIA DEL CARMEN' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'VIDAURRE AGUILAR DE ROSSITER,  KAREN PATRICIA' Then 'GALVEZ, CINTHYA' " +
                                                            "When asesor = 'VIGIL ARGUEDAS,  GEORGINA LUZ' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'VIGO CASTELLANOS,  JENNIFER DEL ROSARIO' Then 'NAVARRETE, DORA' " +
                                                            "When asesor = 'VILA ALEJOS,  LESLY ANGELY' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'VILCA SANTOS,  LOURDES ALICIA' Then 'IZAGUIRRE, ROXANA' " +
                                                            "When asesor = 'VILELA AGUINAGA,  JORGE LUIS' Then 'AGUILAR, JULISA' " +
                                                            "When asesor = 'VILLANUEVA ENRIQUEZ,  CLEYDEE MARYLIA' Then 'LOPEZ, DORA' " +
                                                            "When asesor = 'VILLANUEVA SANCHEZ,  NEISSER YSSENIA' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'VILLANZONA ACOSTA,  STEPHANIE BRIGITTE' Then 'FELIPA, SUMIKO YASBEL' " +
                                                            "When asesor = 'VILLARAN PACHAS,  JESSICA MARIA' Then 'FARFAN, HILDA' " +
                                                            "When asesor = 'VILLEGAS ARENAS,  MARIA LELIANA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'VILLEGAS ARENAS,MARIA LELIANA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'WALDE RENTERIA,  LOURDES DEL PILAR' Then 'TOVAR, PATRICIA' " +
                                                            "When asesor = 'WILLIS ZOEGER,  JOSE EDUARDO' Then 'DIAZ, KARLA' " +
                                                            "When asesor = 'WONG MENDOZA,  MAX MIGUEL' Then 'GALLEGOS VALDEZ,  MARY ANN' " +
                                                            "When asesor = 'XIQUES MEDINA,  YINAELY' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'YACTAYO FELIX,  PERCY DENNIS' Then 'PERALTA, DANIELA' " +
                                                            "When asesor = 'YACUZZI RIVERA,  RAMON FRANCO' Then 'CABRERA, CLAUDIA' " +
                                                            "When asesor = 'YAMAGUCHI BODERO,  DARIO TADASHI' Then 'ZEGARRA, CARMEN' " +
                                                            "When asesor = 'YARLEQUE RUIZ,  VERONICA CAROLINA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'YARLEQUE SULLON,  MARIA STEPHANY' Then 'MOGOLLON, SANDRA' " +
                                                            "When asesor = 'YARROW RAFFO,  GIOVANNA ESTHER' Then 'SANTOS, GIULIANA' " +
                                                            "When asesor = 'YEPEZ SANCHEZ,  CLAUDIA CAROLINA' Then 'TOVAR, PATRICIA' " +
                                                            "When asesor = 'YPANAQUE BALLONA,  MANUEL' Then 'ROJAS,  ZAIDVICT' " +
                                                            "When asesor = 'YRIGOIN FALLAQUE,  CAROLINA DEL ROCIO' Then 'GIL,  CORAL' " +
                                                            "When asesor = 'YUNGO QUISPE,  NORMA' Then 'INFANTE, ERNESTO' " +
                                                            "When asesor = 'ZAMBRANO CHUNGA,  FIORELLA VIANNEY' Then 'YLLATOPA, SAYURI' " +
                                                            "When asesor = 'ZAMORA ARMESTAR,  MIRIAM NOHELY' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ZAPATA DEZA,  ANA MARIA' Then 'PURIZAGA RAMOS,FANNY ELENA' " +
                                                            "When asesor = 'ZAVALA OLIVOS,  ROSA AURORA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ZAVALETA AQUINO,  LUIS ALBERTO' Then 'ACEVEDO, LUCERO' " +
                                                            "When asesor = 'ZEGARRA GONZALES,YESSIE LOURDES' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ZEGARRA PERICHE,  KARINA PATRICIA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ZEGARRA SANCHEZ,  KAREN LIZBHET' Then 'LLERENA, MARCO' " +
                                                            "When asesor = 'ZEGARRA TEJADA,  ANA JULISSA' Then 'GRUPO OTROS, asesorAS' " +
                                                            "When asesor = 'ZUAZO URIBE,  FERNANDO ALEXANDER' Then 'NOVOA, LLANDER' " +
                                                            "else NULL end")) \
                         .withColumn("DES_SEDE",F.expr("Case When asesor = 'ÑIQUE TOCAS,  SHIRLEY MASSIEL' Then 'LIMA 3' " +
                                                             "When Asesor = 'ACUÑA LARA,  SOCORRO MAGDALENA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'AFILIACION ,  VIRTUAL' Then 'SAN BORJA' " +
                                                        "When Asesor = 'AFILIACION , VIRTUAL ' Then 'SAN BORJA' " +
                                                        "When Asesor = 'AGURTO BOUILLON,  CINTHIA MARIA' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'AITA DE CHIRINOS,  ELSA MARIA' Then 'CHICLAYO' " +
                                                        "When Asesor = 'AITA VDA DE CHIRINOS,  ELSA MARIA' Then 'CHICLAYO' " +
                                                        "When Asesor = 'ALARCON FLORES,  JACQUELINE YAHAIRA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ALARCON ROJAS,  GRECIA ELIZABETH' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ALARCON VASQUEZ,  MARIA MAGDALENA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'ALAYO JIMENEZ DE VILCHEZ,  KARINA CONCEPCION' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ALBAN VILLEGAS,  ALEYDA VIANEY' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'ALCANTARA MORENO,  MARIA FIORELLA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'ALFARO ASTETE,  FIORELLA YULIE' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ALFARO AVALOS,  JACKELINE' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'ALMESTAR MAURICIO DE CARRERA,  VANESA' Then 'LIMA 3' " +
                                                        "When Asesor = 'ALT GROUP S.A.C.' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'ALVARADO CHERRE,  ALI ARMANDO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ALVARADO FERNANDEZ,  JOHANNA MILAGRITOS' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'ALVARADO LIZAMA,  JACKELINE MIRELLA' Then 'PIURA' " +
                                                        "When Asesor = 'ALVAREZ RAMOS,  JULIANA ELIZABETH' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ALVAREZ SALERNO,  JUAN CARLOS' Then 'LIMA 3' " +
                                                        "When Asesor = 'ALVEAR LLALLA,  CAROLINA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ALZAMORA CASTILLO,  BRAYAN YAMPIER' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ALZAMORA MORON DE GIL,  MARIA DEL ROSARIO' Then 'LIMA 2' " +
                                                        "When Asesor = 'ALZOLA COHEN,  JOHANNA ALEXANDRA' Then 'LIMA 1' " +
                                                        "When Asesor = 'AMARO MAMANI,  SANDRA YSAVET' Then 'LIMA 2' " +
                                                        "When Asesor = 'ANACLETO JIMÉNEZ,  PATRICIA ELIANA' Then 'LIMA 2' " +
                                                        "When Asesor = 'ANACLETO JIMENEZ,  PATRICIA ELIANA' Then 'LIMA 2' " +
                                                        "When Asesor = 'ANCAJIMA MELENDEZ,  FLOR DE MARIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'ANCIBURO FELIX,  ROSA MATILDE' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ANDRADE ZUÑIGA,  PABLO CESAR' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ANGULO CUEVA,  ROSA ISABEL' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'ANSHIN S.A.C. CORREDORES DE SEGUROS' Then 'SAN BORJA' " +
                                                        "When Asesor = 'ANTICONA VELASQUEZ,  VANESSA KATHERINE' Then 'TRUJILLO' " +
                                                        "When Asesor = 'ANTONIO CUADROS,  CINTYA NATALY' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ARAGON CASTILLO,  VERONICA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ARANIBAR ACUÑA,  DAYANARA ZANAE' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ARANIS GARCIA - ROSSELL,  GABRIEL ANDRES' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ARCE DEL BUSTO,IRIS CAROLINA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'ARESTEGUI CANALES DE RENGIFO,  SONIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'ARROYO RUBIO,  MELISSA BERENISSE' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'ARROYO ZAVALETA,  ANGEL OSWALDO' Then 'LIMA 4' " +
                                                        "When Asesor = 'ARRUE COLLAHUA,  MELANIE AMY ELIZABETH' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ARTHUR J. GALLAGHER PERÚ CORREDORES DE SEGUROS S.A.' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'ASENCIOS ESPINOZA,  GLORIA MARIA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'ASENCIOS ESPINOZA,GLORIA MARIA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'ASENJO FERREYROS,  RENZO ALBERTO' Then 'LIMA 1' " +
                                                        "When Asesor = 'ASESOR ECOMMERCE,  .' Then 'SAN BORJA 2' " +
                                                        "When Asesor = 'ASTETE LOPEZ, LEASOL ALICIA' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'ATLANTIC CORREDORES DE SEGUROS S.A.C.' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'AVALOS PAZ, YOLANDA BEATRIZ' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'AVELLANEDA GUZMAN,  YOVANNY DEL PILAR' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'AVENDAÑO RODRIGUEZ,  ELIZABETH GIANNINA' Then 'LIMA 1' " +
                                                        "When Asesor = 'AYSANOA BALLESTER,  ELIAS RODOLFO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'AYULO PALACIOS,  ANGELA CESAREA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'BACAS QUISPE,ZENAIDA ' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'BALCAZAR FLORES,  MARLENY NADIA' Then 'LIMA 3' " +
                                                        "When Asesor = 'BAMONDE SEGURA,  CARMEN ADELA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BARDALEZ FLORES,  JANE LEE' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'BARRANTES COLETTI,  ANA MARIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'BARRANTES PAREDES,  LILIAN REGINA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'BARRANTES PAREDES, LILIAN REGINA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'BARRIENTOS SILVA,  JACKELINE' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'BARROS DOMINGUEZ,  KAREN MARILU' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BASTANTE LAZARO,  MARIA ESTHER' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'BASTARRACHEA GARCIA,  EMMA JESUS' Then 'PIURA' " +
                                                        "When Asesor = 'BASTOS NINAYAHUAR,  CARMEN ISELA' Then 'LIMA 3' " +
                                                        "When Asesor = 'BASURTO ARAKI,  EDUARDO' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'BAY DE LAZO,  VICTORIA HORTENSIA' Then 'TRUJILLO' " +
                                                        "When Asesor = 'BAY OLAECHEA VDA DE LAZO,  VICTORIA HORTENSIA' Then 'TRUJILLO' " +
                                                        "When Asesor = 'BAYONA FLORES,  MARIA DEL CARMEN' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'BECERRA BROKERS CORREDORES DE SEGUROS SAC' Then 'SAN BORJA' " +
                                                        "When Asesor = 'BECERRA CUEVA,  VANESA SOFIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'BEDON SARANGO,  SISSY MELISSA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BEDREGAL GARCIA,  MARYSABEL VICTORIA' Then 'LIMA 3' " +
                                                        "When Asesor = 'BEJARANO AGUINAGA,  DIEGO ALONSO' Then 'LIMA 2' " +
                                                        "When Asesor = 'BEJARANO BALCAZAR, JULIA ELIZABETH' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'BELTRAME MONTOYA,  JORGE RAUL' Then 'LIMA 1' " +
                                                        "When Asesor = 'BELTRAN LAVANDERA,  MARILYN YOLANDA' Then 'TRUJILLO' " +
                                                        "When Asesor = 'BENAVENTE LEIGH,  MARY SILVIA' Then 'PIURA' " +
                                                        "When Asesor = 'BENAVIDES AYALA,  ALEX MICHAEL' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'BENAVIDES VERGARA,CLARA MARIA IRMA ANA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'BENGOA MARQUINA,  ANELIN MERCEDES' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'BERMEJO GARCIA,  GIULIANA PAOLA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BERNAL TORRES,  NERY DAMARIS' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'BERNUY SILVA,  ROSA JEANNETTE' Then 'TRUJILLO' " +
                                                        "When Asesor = 'BIANCHI BURGA,GIANNINA MARIA DEL CARMEN' Then 'SAN BORJA' " +
                                                        "When Asesor = 'BOCANEGRA MONGRUT,  CRISTIAN ALEJANDRO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BOCANEGRA NUÑEZ DE ANGULO,  YESENIA ROXANA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'BOHORQUEZ HAUYON,  VICTOR ALONSO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BONIFAZ URETA,  MAURICIO GABRIEL' Then 'LIMA 1' " +
                                                        "When Asesor = 'BORDA VEGA,  PATRICIA' Then 'AREQUIPA - CANAL EV' " +
                                                        "When Asesor = 'BORJA ALEJOS,  MONICA CECILIA' Then 'LIMA 1' " +
                                                        "When Asesor = 'BOTTON PANTA,  HELGA MARIELLA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'BOULANGER JIMENEZ,  FIORELLA DEL CARMEN' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BRACAMONTE UGAZ,  LOURDES GORETTI' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'BRAVO HERENCIA,  CARLOS ENRIQUE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BRENNER GOMEZ,  ISABEL CRISTINA' Then 'LIMA 3' " +
                                                        "When Asesor = 'BRETONECHE ,  MERCEDES MARISELLA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BRUNO CORDOVA,  FRESSIA DEL PILAR' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BRUNO CORDOVA, FRESSIA DEL PILAR' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'BUENO RODRIGUEZ,  GLADYS ROSMERY' Then 'TRUJILLO' " +
                                                        "When Asesor = 'BUITRON LOLY,  CARMEN ROSARIO' Then 'SAN BORJA' " +
                                                        "When Asesor = 'BURGA BRAVO,  JULIA MARIBEL' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'BURGOS IBAÑEZ,  CINTHIA VANESSA' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'BUSTAMANTE PRADA,  CESAR AUGUSTO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CABANA DIAZ,  DANICA GABRIELA' Then 'LIMA 2' " +
                                                        "When Asesor = 'CABEZAS ZAMUDIO,  LORENA MARINA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CABRERA CARRILLO,  DIEGO ARTURO' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CABRERA MATUTE,  NIDIA SORAYA' Then 'ICA' " +
                                                        "When Asesor = 'CABRERA MIÑANO,  MARIA DEL PILAR' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'CACERES VARGAS,CARMEN ASTRID' Then 'SAN BORJA' " +
                                                        "When Asesor = 'CACHAY SAAVEDRA,  EVELIN JANETT' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CAJUSOL CHEPE,ROSA NEGDA' Then 'TRUJILLO' " +
                                                        "When Asesor = 'CALDAS VALVERDE,  ORFA TABITA' Then 'LIMA 2' " +
                                                        "When Asesor = 'CALDAS ZAPATA,  JORGE RAUL' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CALDERON VASQUEZ,  LIDYA LISSETTE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CALLE TAVARA,  PATRICIA MARIA DEL CARMEN' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'CAMPOS ARAMBULO,  JHONY LEONEL' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CAMPOS CASTILLO,  VANESSA' Then 'TRUJILLO' " +
                                                        "When Asesor = 'CAMPOS MORENO,  SANDRA CONSUELO' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'CANAL ,  RECUPERO INDIRECTO NR' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CANAL RECUPERO,  DIRECTO NR' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CANAL RECUPERO, DIRECTO NR ' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CAQUI TELLO,  JOSE MIGUEL' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CARDOZA LICAS,  VALERY ROXANA' Then 'LIMA 3' " +
                                                        "When Asesor = 'CARDOZO HONORIO,  MAYRELLI GEANNI' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CARNERO MORA,  ELVIRA VICTORIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CARO AQUINO,  CESAR MANUEL' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CARO SALAZAR DE GUANILO,  MILAGRITOS CINTHIA ELIZABETH' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CARPIO QUINTASI,  REDY MAURICIO' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CARPIO VELARDE,  ROCIO MIRYAM' Then 'AREQUIPA - CANAL EV' " +
                                                        "When Asesor = 'CARRANZA CASTAÑEDA,MARGOT ' Then 'SAN BORJA' " +
                                                        "When Asesor = 'CARRANZA RAMIREZ,  JAIME GREGORIO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CARRASCO CORRALES,  ANA CECILIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CARRILLO LEANDRO,  MARIA DEL ROSARIO' Then 'LIMA 1' " +
                                                        "When Asesor = 'CARRION GUEVARA,  KIHARA STEPHANY' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'CARRION SEBASTIANI,  LOURDES DEL CARMEN' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'CASTAÑEDA CERCADO,AGUEDA ' Then 'SAN BORJA' " +
                                                        "When Asesor = 'CASTILLO CRUZ,  SANDY KATHERINE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CASTILLO HERNANDEZ,  JAIME RICHARD' Then 'LIMA 2' " +
                                                        "When Asesor = 'CASTILLO HUAMAN,  YOVANI ESPERANZA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CASTILLO JHON,  ADA JACKELYN' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CASTILLO MIMBELA,  GISELLA YVETTE' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CASTILLO PISCONTE,  SANDRA DEL PILAR' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CASTILLO SAAVEDRA,  GLADYS TANIA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'CASTILLO SUSANIBAR,  ANGEL ANTONIO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CASTRAT FIEGE,  MARIA DEL PILAR' Then 'SAN BORJA' " +
                                                        "When Asesor = 'CASTRO GASTELO,  THALIA CARMENROSA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CASTRO RAMOS,  GERALDINE DEL CARMEN' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'CASTRO ZUÑIGA,  GEORGINNA ALEXANDRA' Then 'LIMA 3' " +
                                                        "When Asesor = 'CCAMA MAMANI,  NARVI DENIS' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CCAULLA HUAMACCTO,  YENY AURELIA' Then 'LIMA 2' " +
                                                        "When Asesor = 'CELIS VENTURA,  MERSI SUJEY' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'CERNA ZAMORA,  CARMEN VICTORA' Then 'LIMA 1' " +
                                                        "When Asesor = 'CESPEDES CAJO,  NATALIA DEL JESUS' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CHACON LOPEZ,  KATHERINE FABIOLA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'CHAMAN CHANG,  MIRTHA DEL ROCIO' Then 'TRUJILLO' " +
                                                        "When Asesor = 'CHAMBI LIPE,  MARIBEL BETZABET' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CHANAME BRANDIZZI,  SONIA JOHANNA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CHANG SALAZAR,  JULISSA MARGARETT' Then 'LIMA 2' " +
                                                        "When Asesor = 'CHAVEZ FARROMEQUE,  ANDREA NATALIA' Then 'LIMA 1' " +
                                                        "When Asesor = 'CHAVEZ HURTADO,  MARIA PAZ' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'CHAVEZ MENDOZA,  FANNY ROXANA' Then 'LIMA 1' " +
                                                        "When Asesor = 'CHIMPEN MIMBELA,  RONALD LUIS' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CHIROQUE GARCIA,  FAYSSULY ELIBETH' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'CHUECA REQUENA,  INGRID GIANNINA' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'CHUMPITAZ ORTEGA,  KAROLINE CRISTAL' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CIEZA FERNANDEZ,  NANCY' Then 'LIMA 3' " +
                                                        "When Asesor = 'CISNEROS SCHUAVEZ,  MARIA JOSE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CJURO RAMOS,  MIRIAM MILAGROS' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CODIGO RECURRENTE CANAL,  DIRECTO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CODIGO RECURRENTE CANAL,  INDIRECTO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CODIGO RECURRENTE CANAL, DIRECTO ' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CODIGO RECURRENTE CANAL, INDIRECTO ' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'COICO SUYON,  KATHERIN DOMINGA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'CONSEJEROS Y CORREDORES DE SEGUROS SAC' Then 'SAN BORJA' " +
                                                        "When Asesor = 'CORDERO DIAZ,  FIORELLA MILAGROS' Then 'LIMA 2' " +
                                                        "When Asesor = 'CORDOVA CHACONDORI,  ANGIE DAISY' Then 'LIMA 1' " +
                                                        "When Asesor = 'CORDOVA ESCOBAR,  GLADYS ROSSANA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'CORDOVA FLORIANO,  SANDY ROSA' Then 'LIMA 2' " +
                                                        "When Asesor = 'CORDOVA VICENTE,  GABRIEL' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CORRALES BENITES,  CAROL GRACE' Then 'LIMA 3' " +
                                                        "When Asesor = 'CORREA GUEVARA,  ALEXANDER' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CORREA NORIEGA,  ROSITA FLOR' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'CORREA SAENZ,  DORA JUANA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'CORREDORES DE SEGUROS FALABELLA S.A.C.' Then 'SAN BORJA' " +
                                                        "When Asesor = 'COTRINA PALACIOS,  JINNY KATHERINE' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'CRUZ RODRIGUEZ,  DENIA VANESSA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'CRUZ ZUMAETA,  JOSSELYN GERALDINE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'CUCHO REJAS,  GRESIA GISBEL' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'CUENCA GOMEZ,  SANDRA GEOVANI' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'CUEVA MASABEL,  IVONNE ALEXANDRA' Then 'LIMA 1' " +
                                                        "When Asesor = 'CUSTODIO ARTEAGA,  VALERIA ALEXANDRA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'CUYA CUYA,  STEPHANIE BRISSETTE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'DE LA CRUZ HONORES,  ROSSMARY' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'DE LA CRUZ MEDINA,  MONICA FARAH' Then 'LIMA 3' " +
                                                        "When Asesor = 'DE LA PUENTE GONZALES DEL RIEGO,  JULIA MARIA' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'DE LA ROSA MOROTE,  MARIA LETICIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'DE LOS RIOS MARQUEZ,  CORINA YASMIN' Then 'LIMA 3' " +
                                                        "When Asesor = 'DE LOS RIOS MONCADA,VIOLETA ' Then 'SAN BORJA' " +
                                                        "When Asesor = 'DEDUCCION VENTA ,  NO RECURRENTE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'DEDUCCION VENTA ,  RECURRENTE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'DEDUCCION VENTA , NO RECURRENTE ' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'DEL CASTILLO PANCORVO,  JULIA ELENA' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'DEL VALLE PORTAL,  SARA' Then 'LIMA 1' " +
                                                        "When Asesor = 'DELGADO AGUILAR,  MARIA GUISELA' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'DELGADO ALVAREZ,  CLAUDIA ALEJANDRA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'DEZA MENDOZA,  KAREN STEFANY' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'DIAZ CUCHO,  HERBERT ALBERTO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'DIAZ FERNANDEZ,  GLADYS DEL PILAR' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'DIAZ ORIBE,  ROSA MARGARITA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'DIESTRA ALAYO DE RODRIGUEZ,  MARIA RAQUEL' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'DIESTRA TRINIDAD,  MAYDA VANESA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'DIOS CISNEROS,  PATRICIA MERCEDES' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'DIOS CISNEROS, PATRICIA MERCEDES' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'DIOSES MONASTERIO,  STEFFANY' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'DURAN RODRIGUEZ,  MARIA DEL ROSARIO' Then 'LIMA 2' " +
                                                        "When Asesor = 'EGO AGUIRRE ESPINOZA,ROSA SARA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'ELIAS NAVARRETE DE RIZO PATRON,  ROCIO DEL CARMEN' Then 'SAN BORJA' " +
                                                        "When Asesor = 'EMP DE SERVICIOS MULTIPLES UNIVERSAL S.R.L.' Then 'TRUJILLO' " +
                                                        "When Asesor = 'ESCALANTE CUYA,  ERIKA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ESCOBEDO ZUÑIGA,  EDGAR ERNESTO ISAIAS' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ESCUDERO VELASQUEZ,NORMA ' Then 'SAN BORJA' " +
                                                        "When Asesor = 'ESCURRA POLO,  ELVIRA AMANDA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ESPEJO GONZALES,  JULISSA AIDA' Then 'TRUJILLO' " +
                                                        "When Asesor = 'ESPINAL GUTIERREZ,  ALEJANDRA PENELOPE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ESQUERRE QUISPE,  ROSA ISABEL' Then 'LIMA 1' " +
                                                        "When Asesor = 'ESQUIVEL CABRERA,  ELIZABETH JOHNNY' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ESTEBAN ARELLANO,  CARMEN ZENAIDA' Then 'LIMA 2' " +
                                                        "When Asesor = 'ESTELA TORRES,  RAQUEL' Then 'CHICLAYO' " +
                                                        "When Asesor = 'FABIAN MORI,  TERESA' Then 'CHICLAYO' " +
                                                        "When Asesor = 'FALCON RAMIREZ,  PAOLA CONSUELO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'FALZONE ESPINOZA,  NICOLE JOLIE' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'FARFAN GRANADINO,  FIORELLA YESENIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'FARFAN URDANEGUI,  KATHERINE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'FARIAS ARCA,  IRMA ELIZABETH' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'FARRO MANSILLA,  SARA MELISSA' Then 'LIMA 2' " +
                                                        "When Asesor = 'FEBRES PORTAL,  NATHALIE PAMELA' Then 'LIMA 1' " +
                                                        "When Asesor = 'FERNANDEZ ABAD,  RUTH ELIZABETH' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'FERNANDEZ AGREDA,  GIULIANA VICTORIA' Then 'CHICLAYO' " +
                                                        "When Asesor = 'FERNANDEZ CASTILLO,  WENDY MARCELA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'FERRE VELASQUEZ,  ELIZABETH KATERINE' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'FERRO SEMINARIO,  PAOLA MARIELLA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'FICEFA S.A.C' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'FIESTAS VALLADARES,  CECILIA GENARA' Then 'LIMA 1' " +
                                                        "When Asesor = 'FIGUEROA MANRIQUEZ, ANDREA STEFANIA' Then 'LIMA 1' " +
                                                        "When Asesor = 'FLORES BENDEZU,  CLAUDIA MARILYN' Then 'LIMA 2' " +
                                                        "When Asesor = 'FLORES FLORES,  SEGUNDO' Then 'PIURA' " +
                                                        "When Asesor = 'FLORES TORRES,  GIANCARLO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'FLORES VASQUEZ, MIRYAM YANET' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'FLORES VIGO,  KELLY JUDITH' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'FLORINDEZ SEGURA,  GLORIA ISABEL' Then 'SAN BORJA' " +
                                                        "When Asesor = 'FOSCA GAMBETTA,  STEFANO GIUSEPPE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'FRANCO RAFAEL,  ROSA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'FREYRE SOLARI,  CECILIA HILDA' Then 'LIMA 2' " +
                                                        "When Asesor = 'FRIAS BILLINGHURST,  LIZ MARICELA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'FRIAS CRUZ,  JUAN CARLOS' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'FUENTES PEÑA,  MARIA ADRIANA DEL ROSARIO' Then 'SAN BORJA' " +
                                                        "When Asesor = 'FYF DARUICH CORREDORES DE SEGUROS SAC' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'GAGO SILVA,  MARIA ELENA' Then 'LIMA 3' " +
                                                        "When Asesor = 'GALARZA POMASUNCO,  YESSICA CLAUDIA' Then 'LIMA 1' " +
                                                        "When Asesor = 'GALLARDO SUSANO,  SHARON CLARA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'GALLEGOS VALDEZ,  MARY ANN' Then 'CHICLAYO' " +
                                                        "When Asesor = 'GALVEZ TORRES,  ROSALIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'GALVEZ VALERA,  MARGARITA' Then 'LIMA 2' " +
                                                        "When Asesor = 'GAMALLO BALCAZAR DE REYNA,  DANIELLA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'GAMARRA FUSTAMANTE,  JORGE GORIK' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'GAMARRA LUJAN,  MAYCOL JESUS VICTOR' Then 'LIMA 1' " +
                                                        "When Asesor = 'GAMARRA VERGARA,  CECILIA ADELA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'GAMIO ALBURQUEQUE,  CLAUDIA ZARELA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'GARATE MONDRAGON,  KARLA JACKELINE' Then 'LIMA 3' " +
                                                        "When Asesor = 'GARCIA CASTILLO,  DELLY' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'GARCIA CUMPA,  ALMA MAGDALENA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'GARCIA DELGADO,  ROUTH ELITH' Then 'CHICLAYO' " +
                                                        "When Asesor = 'GARCIA ORMEÑO, NELLA VICTORIA' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'GARCIA SOTO,  VICTOR LUIS' Then 'LIMA 1' " +
                                                        "When Asesor = 'GARCIA VALLADARES,  MARIA GUISELA' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'GARCIA ZAPATA,  ANIA ISABELLA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'GAYOSO NUÑEZ,  ANUSKA ZUSSETTI' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'GIL CAMPOS,  ANA PATRICIA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'GIL ZEVALLOS,  CARLA GABRIELA' Then 'ICA' " +
                                                        "When Asesor = 'GOMERO CESPEDES DE GOMEZ,ELBIA NANCY' Then 'SAN BORJA' " +
                                                        "When Asesor = 'GOMES ROJAS,  KAREN ROSSMERY' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'GOMEZ ARROYO,  KATIUSKA PAOLA' Then 'LIMA 2' " +
                                                        "When Asesor = 'GOMEZ CAVERO,  JUAN GUILLERMO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'GOMEZ GUZMAN,  CLAUDIA ALEJANDRA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'GONZALES CRUZ,  YURICO YUVISSA NATHALY' Then 'LIMA 3' " +
                                                        "When Asesor = 'GONZALES DIAZ,  MARIBEL' Then 'LIMA 2' " +
                                                        "When Asesor = 'GONZALES GARCIA,  ROCIO ESMERALDA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'GONZALES MELGAR,  JACQUELYN LORENS' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'GONZALES PAUCAR,  JACKELINE YAMALI' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'GONZALES ZUÑIGA,  ANISA RACHIDA' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'GONZALES ZUÑIGA,  MARIELLA EMILIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'GOVEA LOPEZ DE DE LA BARRERA,LAURA ' Then 'SAN BORJA' " +
                                                        "When Asesor = 'GOYBURO ORTIZ,  JENNEIFER JACQUELYN' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'GRADOS VILCAPOMA,  ROSARIO SOLEDAD' Then 'LIMA 2' " +
                                                        "When Asesor = 'GRANDY ALCALDE,  LISETTE MILAGROS' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'GRIMALDI PELTROCHE,  PERCY' Then 'LIMA 1' " +
                                                        "When Asesor = 'GUERRERO LEGUA,  OSCAR ORLANDO' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'GUERRERO URPEQUE,  ERICK FABIAN' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'GUEVARA ALPACA,  JOSHUA ANDRE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'GUEVARA MENDOZA,  MERY ISABEL' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'GUILLEN IBARCENA,  SHIRLEY GIANNINA' Then 'AREQUIPA - CANAL EV' " +
                                                        "When Asesor = 'GUILLEN MENDOZA,  LAURA PATRICIA' Then 'LIMA 2' " +
                                                        "When Asesor = 'GUILLEN NEYRA,  ROSA KARINA' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'GUTIERREZ ALVARADO,ANGELINA MAGDALENA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'GUTIERREZ ARTEAGA,  LUZ MILAGROS' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'GUTIERREZ MARROQUIN,  ANA ESTEFANI' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'GUTIERREZ MEDRANO,  ROXANA IRIS' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'GUZMAN PEREZ DE LATURE,  ROSA KARINA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'GUZMAN PEREZ DE LATURE, ROSA KARINA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'HANCO GARCIA,  DAYANN LIZETH' Then 'AREQUIPA - CANAL EV' " +
                                                        "When Asesor = 'HERMOZA VINCES, GONZALO FERNANDO' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'HERNANDEZ MORENO,  GRACIELA EUGENIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'HERRERA RIOS DE VEGA,ELVA ROSA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'HOLGUIN CRUZ,  GIOVANA DEL CARMEN' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'HUAMAN ARBULU,  GIULLIANA PATRICIA DEL CARMEN' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'HUAMAN FARFAN,  NURY MARCIA' Then 'LIMA 1' " +
                                                        "When Asesor = 'HUAMANCHUMO RAMIREZ,  MARIA ANGELICA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'HUANATICO PINTO,  NORMA' Then 'AREQUIPA - CANAL EV' " +
                                                        "When Asesor = 'HUERTAS MOGOLLON,  HOLDAVID AURELIO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'HURTADO TEMBLADERA,  JANET MAYELA' Then 'HUANCAYO - CANAL EV' " +
                                                        "When Asesor = 'HURTADO VALDIVIA,  BRAYAN PAUL' Then 'LIMA 2' " +
                                                        "When Asesor = 'INECV E.I.R.L' Then 'PIURA' " +
                                                        "When Asesor = 'INFANTE PRIAS,  LORENA PAOLA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'IRUJO DEL AGUILA,  MILUSKA ANTONIETA' Then 'SAN BORJA 2' " +
                                                        "When Asesor = 'ITAL SEGUROS SA CORREDORES DE SEGUROS' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'JIMENEZ MERINO, LINDSAY KATHERINE' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'JULCA OLANO,  MACYORI MISHELL' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'JULCAHUANCA JIMENEZ,  ANITA ELVIRA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'KAM LEON,  CINTHYA BEATRIZ' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'KONECTA DIGITAL' Then 'SAN BORJA' " +
                                                        "When Asesor = 'LA POSITIVA SEGUROS Y REASEGUROS S.A.A. - BANCO PICHINCHA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'LA PROTECTORA CORREDORES DE SEGUROS S.A.' Then 'SAN BORJA' " +
                                                        "When Asesor = 'LA ROSA ALVAREZ,  FELIX FERNANDO' Then 'LIMA 1' " +
                                                        "When Asesor = 'LADINES SAAVEDRA,  MARITZA ESTHER' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LANDAVERI GRIMALDO,  JOSE ALEJANDRO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LAVERIAN OTERO,  NELIDA VANESSA' Then 'LIMA 3' " +
                                                        "When Asesor = 'LAZO MERINO,  LUIS GUSTAVO' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'LEGOAS ORDOÑEZ,  NELSON KLISMAN' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LEIVA PALACIOS,  MERCEDES ROSARIO' Then 'LIMA 1' " +
                                                        "When Asesor = 'LEON DINCLANG,  MARGARITA MANUELA' Then 'LIMA 3' " +
                                                        "When Asesor = 'LEON MEDINA,  ELIZABETH YURI' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'LEON OBALLE,  MARIA VERONICA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'LINARES VILLACORTA,  CINTHYA ISABELA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LIZARDO PEREZ,  SAGRARIO DE LOS ANGELES' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LLERENA VARGAS,  MARCO ALONSO' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'LLOSA GAMARRA,  JHOAN LISSETH' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LLUEN GASTELO,  VANESSA ELIZABETH' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LLUNCOR MAYANGA,  DIEGO ARNALDO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LOCHER PRECIADO,  FRANCESCA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'LOPEZ CASTILLO,  VERONICA ELIZABETH' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'LOPEZ FARRO,  GISELA ELIZABETH' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LOPEZ GAMONET,  JOHANNA ALEXANDRA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'LOPEZ GUZMAN,  HELEN YULMAR' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'LOPEZ MARCILLA,  MARIA TERESA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'LOPEZ PEÑA,  KERLY EVELYN' Then 'LIMA 3' " +
                                                        "When Asesor = 'LOPEZ VALDEZ,  LUIS ENRIQUE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LOZADA VIERA,  CARMEN JULIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'LUCCHINI LAZARO,  FERNANDO JOALDO' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'LUJAN CAVERO,  CLAUDIA NOEMI' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'LUNA BARRAZA,  SANDRA MARIELA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'MACARLUPU TORRES,  ANA CINTHIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MACEDO SAIRITUPA,  JESSICA ROSMERY' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MADRID RUMICHE,  ROSA SOFIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'MALCA ESPINOZA,  GLORIA INES' Then 'SAN BORJA' " +
                                                        "When Asesor = 'MAMANI MAR, MARIA ISABEL' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'MANCHEGO NUÑEZ ROBLES DE BALLON,  CARMEN LOURDES' Then 'AREQUIPA' " +
                                                        "When Asesor = 'MANRIQUE NAVARRO,  JUANA JAQUELINE' Then 'LIMA 2' " +
                                                        "When Asesor = 'MANTILLA MEZA,  ALAN GABRIEL' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'MANZO SANTOS,  EVA DINA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MAQUERA GAMBETTA,  ARACELI YAMILET' Then 'LIMA 2' " +
                                                        "When Asesor = 'MARCELO PEREZ DE DELGADO,  MERCEDES JULISSA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'MARES CARRILLO,  ALDANA PEGGY' Then 'LIMA 2' " +
                                                        "When Asesor = 'MARIN FLORES,  SANDRA' Then 'CHICLAYO' " +
                                                        "When Asesor = 'MARQUEZ ESPINOZA,  ROXANA BEATRIZ' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MARRO CABEZAS,  CARMEN MARTHA' Then 'LIMA 1' " +
                                                        "When Asesor = 'MARSH REHDER S.A.C. CORREDORES DE SEGUROS' Then 'SAN BORJA' " +
                                                        "When Asesor = 'MARTINEZ ENCALADA,  CECILIA FRANCISCA' Then 'LIMA 3' " +
                                                        "When Asesor = 'MASCARO VENTO,  LUCY VICTORIA' Then 'LIMA 3' " +
                                                        "When Asesor = 'MASSA GARCIA,  MARTHA ELENA' Then 'ICA' " +
                                                        "When Asesor = 'MATHEUS AGUIRRE, GABRIELA LUCILA' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'MATIENZO RAMIREZ,  MAYRA CINTHIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MAURICIO BACA,  ROCIO VIVIANA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'MAURICIO GARRIDO,  SANDRA MARINA' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'MAUTINO CARBAJAL,  GIANNINA MIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'MAVAC CORREDORES DE SEGUROS S.A.C.' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'MAYORGA MEZA,  SANDRA MILAGROS' Then 'LIMA 3' " +
                                                        "When Asesor = 'MAYTA MARROQUIN DE ESCUDERO,  MARIA EUGENIA' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'MAZA ROMERO,  TATIANA MARJHORET' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'MAZULIS CHAVEZ,  CAROLINA GYSLEIN' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'MAZZEI HOLGUIN,  VANESSA ILIANA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MEDINA ELGUERA,  PAMELA ROSARIO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MEDINA MONAR, MARLENE JOSEFA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'MEDINA QUIROZ,  INES CAROLINA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'MEDINA VIVANCO DE LAGUNA,  PATRICIA IVONNE' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'MEIGGS MENDOZA,  BRUNO ANTONIO MARTIN' Then 'LIMA 1' " +
                                                        "When Asesor = 'MELENDEZ MIRANDA,  JHON FRANKLIN' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MELLY USQUIANO DE SAMAME,  CATALINA ISABEL' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'MENDEZ CANALES,  VIRGINIA DORA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'MENDEZ FAJARDO,  CLAUDIA FIORELLA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'MENDOZA ACOSTA,PATRICIA LILY' Then 'SAN BORJA' " +
                                                        "When Asesor = 'MENDOZA DE LA PEÑA,  KATHERINE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MENDOZA HEREDIA,  MURYELL ARIANE' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'MENDOZA ORTIZ,  JIMMY SMITH' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'MENDOZA RAMOS,  IVONNE MARCELA' Then 'LIMA 2' " +
                                                        "When Asesor = 'MENDOZA SILVA,  KAREN ANGELICA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'MENDOZA VASQUEZ,  ANAI DEL SOL' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'MENDOZA VASQUEZ, ANAI DEL SOL' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'MERCADO PARIONA,  LYZ EVELYN' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MERINO CALERO,  MARY KELLY' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'MESONES LINARES,  JULISSA ELIANA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'MIÑAN ABAD,  JOHANA CAROLINA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MICHILOT HUERTAS, ADA GUISELLA' Then 'PIURA' " +
                                                        "When Asesor = 'MIRANDA CASTRO,  KATY NOELIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MOGOLLON CARMEN,  SANDRA MILUSKA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'MOGROVEJO ARCE DE AGUILAR,  RENATA ISELA' Then 'PIURA' " +
                                                        "When Asesor = 'MONCADA GARCIA,  ANDREA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MONTALVAN BARRETO,  GLADYS CATHERINE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MONTENEGRO ROMAN,  MARIA ESTHER' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'MONTERO AGUILA,  MARIA FERNANDA' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'MONTERO BERAMENDI,  MARIA DEL CARMEN' Then 'LIMA 4' " +
                                                        "When Asesor = 'MONTERO QUINTANA,  FABIOLA MIRELLA' Then 'LIMA 2' " +
                                                        "When Asesor = 'MONTES GONZALES DE GALINDO,SILVIA ANA' Then 'TRUJILLO' " +
                                                        "When Asesor = 'MONTEZA GONZALES,  MAYRA YESSENIA' Then 'LIMA 3' " +
                                                        "When Asesor = 'MORAN MAURICIO DE LOYOLA,  MAGDALENA MARIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'MORAN TINOCO,  MERCY ESTANITA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'MORENO MONTALVO,  LUZ TRINIDAD' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'MORENO RUIZ,  JOAO MANUEL' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MORENO SOLORZANO,  YESSENIA JHETALLY YAJAIRY' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'MORENO TANTAVILCA,  ESTEYSI SILVIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MOSTACERO RODRIGUEZ,  FIORELLA DEL CARMEN' Then 'LIMA 1' " +
                                                        "When Asesor = 'MUCHA ARIAS,  PATRICIA ELIZABETH' Then 'LIMA 2' " +
                                                        "When Asesor = 'MUCHA QUISPE,  MARLENE MARIBEL' Then 'LIMA 2' " +
                                                        "When Asesor = 'MURILLO ALVA,  ADRIANA PATRICIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'MYASES E.I.R.L' Then 'PIURA' " +
                                                        "When Asesor = 'NAKANDAKARI SHIMABUKURO,ANA ' Then 'SAN BORJA' " +
                                                        "When Asesor = 'NAVARRO DELGADO,ELVIA DEL MILAGRO' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'NAVARRO NIQUIN,  JOHANNA BRENDA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'NAVARRO REY SANCHEZ,  FIORELLA DEL CARMEN' Then 'LIMA 4' " +
                                                        "When Asesor = 'NAZARIO CHAVEZ,  MARIA DEL CARMEN' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'NECIOSUP CHINCHAY,  OSCAR DANY' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'NEWELL MENESES,  WENDY PAMELA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'NEWELL MENESES, WENDY PAMELA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'NEYRA AREVALO,  ANITA CLAUDIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'NIEVES FALLA,  PIERINA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'NOÑUNCA CRISPIN,  ANGGEL LIZBETH' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'NORIEGA SANCHEZ,  ROCIO DEL PILAR' Then 'LIMA 1' " +
                                                        "When Asesor = 'NORIEGA TABORI VDA DE CAMPODONICO,ELISA LILIANA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'NOVOA MENDOZA,  WILBERT MANUEL' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'NUÑEZ DEL PRADO REYNOSO,  JUAN JESUS' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'NUÑEZ GUZMAN,  MARIA SOLEDAD' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'OBANDO FLORES,  DEYSI RUBI' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'OBISPO CASTRO,  JULISSA KATHLEEN' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'OCHANTE BERNARDINO,  JESSICA' Then 'LIMA 1' " +
                                                        "When Asesor = 'OJEDA QUIROZ,  FELIPE MOISES' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'OLIVARES DENEGRI,  JORGE LUIS' Then 'LIMA 3' " +
                                                        "When Asesor = 'OLIVARES INFANTES,  MARCO ANTONIO' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'OLORTIGA CUADRADO,  CINTYA SOLEDAD' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ORE TORRES,  DAYANNA JAZMIN' Then 'LIMA 3' " +
                                                        "When Asesor = 'ORMEÑO PANTA,  ORIANA STEPHANIE' Then 'LIMA 1' " +
                                                        "When Asesor = 'OROZCO JIMENEZ,  ROCIO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ORTIZ VELASCO,  MARIA JANICE' Then 'LIMA 2' " +
                                                        "When Asesor = 'OSORIO VALDEZ,  MARIA ROXANA' Then 'LIMA 4' " +
                                                        "When Asesor = 'OSSIO PORRAS,  LOURDES PELAYA' Then 'LIMA 1' " +
                                                        "When Asesor = 'OYARCE LUDEÑA,  BRUNELLA ANTOANNE' Then 'LIMA 3' " +
                                                        "When Asesor = 'PACHECO BADOINO,  MARIANELLA GUISEPPINA' Then 'AREQUIPA - CANAL EV' " +
                                                        "When Asesor = 'PACORA VASQUEZ,  MILAGROS' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PADILLA BRAVO,  JESSICA PAMELA ROSARIO' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PADILLA USQUIANO,  JUDITH ROSARIO' Then 'LIMA 1' " +
                                                        "When Asesor = 'PAIVA LA ROSA,  SARA JANNET' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PALACIOS HERRERA, OLGA ANGELICA' Then 'ICA' " +
                                                        "When Asesor = 'PALAS TAPIA,  MARIA ELENA DE LOS MILAGROS' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'PALOMINO CACERES,  LUZ ELENA' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'PALOMINO RAMIREZ, MARLENY' Then 'PIURA' " +
                                                        "When Asesor = 'PANTA DIAZ,  JACKSON MARTIN' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PANTA VELEZMORO,  DIANA JATSUMI' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PANTIGOSO LEON,  JHASHAIRA MILAGROS' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PARKER RENGIFO,  LAURA MONICA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PASARA VELASQUEZ, CARMEN CLOTILDE' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'PASARA VELASQUEZ,CARMEN CLOTILDE' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'PASTOR FLORES,  VIVIANA ISABEL' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'PAUCAR RAMIREZ,  CAROL KATHERINE' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'PEÑA MELENDRES,  GIANFAVIO JOSHUA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PEÑA PAJARES,  GISELA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'PELLA ADRIANZEN,  INGRID MILAGROS' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'PENNY BIDEGARAY,  RICHARD JUAN' Then 'SAN BORJA' " +
                                                        "When Asesor = 'PENNY BIDEGARAY,  RICHARD JUÁN JOSÉ DE LA CRÚZ' Then 'SAN BORJA' " +
                                                        "When Asesor = 'PERALTA OLIVARES,  YRINA PAOLA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'PERALTA WALTER, MONICA ALEJANDRA' Then 'CHICLAYO' " +
                                                        "When Asesor = 'PEREYRA DE MORALES,  OLINDA FIDELIA' Then 'TRUJILLO' " +
                                                        "When Asesor = 'PEREZ BECERRA,  JULIO JEISER' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PEREZ BEDOYA,  LETICIA MONICA' Then 'AREQUIPA - CANAL EV' " +
                                                        "When Asesor = 'PEREZ HUAMAN,  IRMA MARIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'PEREZ OSORIO,  ELSA MERCEDES' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PEREZ WALDE,  ADRIANA ANNABELL' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'PINADO LOPEZ DE FACUNDO,  EVELYN GERALDINE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'PINADO LOPEZ,  EVELYN GERALDINE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'PINEDO REATEGUI,CLEDY LUZMILA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'PINILLOS RUSSO,  GIULIANA TERESA' Then 'TRUJILLO' " +
                                                        "When Asesor = 'PINTO BENITES,  ROXANA MARGOTH' Then 'LIMA 2' " +
                                                        "When Asesor = 'PINTO BOHORQUEZ, MIRELLA ' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PINTO PEÑA,  ANDRE ADEMIR ALFREDO' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PISCOYA VENTURA,  MARIA MAGDALENA DE JESUS' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PLAZA VIDAURRE,  HILDA TERESA JACINTA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'POLO REAÑO,  CARLOS MANUEL' Then 'LIMA 1' " +
                                                        "When Asesor = 'POMA PALOMINO,  ROLANDO JAMES' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'POMA PEREZ DE MONTALVO,  KATHIE ISABEL' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'POMEZ TICONA,  MIGUEL ANGEL' Then 'LIMA 1' " +
                                                        "When Asesor = 'PONCE CACEDA,  DIANA ELIZABETH' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'PORTALES JARA,  ERICK JAVIER' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'PRATO NARANJO,  GABRIELA ELENA TINA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'PREVESALUD E.I.R.L.' Then 'TRUJILLO' " +
                                                        "When Asesor = 'PRIETO HERRERA,  ANYIE EDITH' Then 'ICA' " +
                                                        "When Asesor = 'PROLIFE COMERCIALES SAC' Then 'MULTI 1' " +
                                                        "When Asesor = 'PULGAR CASTILLO,ELIZABETH GRACIELA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'PUMA CALLE,  ANGELICA MURIELL' Then 'LIMA 3' " +
                                                        "When Asesor = 'PUPUCHE GAMARRA,  YECENIA PAOLA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'PYE SOLARI DE GONZALEZ,  JANE LOUISE' Then 'SAN BORJA' " +
                                                        "When Asesor = 'QUEZADA OLIVEIRA,  NOELIA NORMA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'QUIÑONES CASTILLO,  JESUS FERNANDO' Then 'LIMA 2' " +
                                                        "When Asesor = 'QUIÑONES  PARIONA,  LUIS JERSSI' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'QUINTANA GUEVARA,  GISELLA ELENA' Then 'LIMA 3' " +
                                                        "When Asesor = 'QUIROZ RIOS,  GABRIELA ELIZABETH' Then 'SAN BORJA' " +
                                                        "When Asesor = 'QUIROZ RIOS, GABRIELA ELIZABETH' Then 'SAN BORJA' " +
                                                        "When Asesor = 'QUISPE POZO,  SOLENA GISELLE MERCEDES' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'RABANAL CONTRERAS,  ALICIA JESUS' Then 'LIMA 1' " +
                                                        "When Asesor = 'RAMIREZ AÑAZGO,  ERIKA VANESSA ADALIA' Then 'LIMA 2' " +
                                                        "When Asesor = 'RAMIREZ QUIJANDRIA,  ADRIANA PATRICIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'RAMIREZ QUIJANDRIA, ADRIANA PATRICIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'RAMIREZ RODRIGUEZ,  MARISA ROSARIO' Then 'PIURA' " +
                                                        "When Asesor = 'RAMIREZ SALAZAR,  CINTHYA HELENA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'RAMOS CAYO,  SILVIA ROSSANA' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'RAMOS DIAZ,  CECILIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'RAMOS LAOS,  TANIA MELISSA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'RAMOS LOZANO,  CARLOS EDUARDO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'RAMOS RUIZ,  SARA GUISELA' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'RAMOS SAENZ,  ORLANDO DANIEL' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'RAMOS VASQUEZ,  ERIN OMAR' Then 'LIMA 1' " +
                                                        "When Asesor = 'RAMOS VASQUEZ, ERIN OMAR' Then 'LIMA 1' " +
                                                        "When Asesor = 'RAZURI GONZALES,  ANDREA YSABEL' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'REAÑO VELASQUEZ,  MIRTHA ELIZABETH' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'REATEGUI PEREZ,  KARLA PAOLA' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'REINOSO PACHECO DE CONTRERAS,  JACQUELINE SUSANNE' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'REQUEJO YALTA,  CAROLINA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'REVOREDO LLANOS, MARIA ELIZABETH' Then 'TRUJILLO' " +
                                                        "When Asesor = 'REYES BALAREZO,  NAHOMI DE LA PAZ' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'REYES CALLE,  VALERIA AURORA' Then 'LIMA 3' " +
                                                        "When Asesor = 'REYES GUTIERREZ,  MARIA PATRICIA' Then 'LIMA 2' " +
                                                        "When Asesor = 'REYES OCAMPO,  TERESA VICTORIA' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'REYES UGAZ,  ANGELA LUCIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'RIOS PALOMINO,  ERICA MELINA' Then 'LIMA 1' " +
                                                        "When Asesor = 'RIOS QUIROZ,  SARA CELIA' Then 'LIMA 4' " +
                                                        "When Asesor = 'RIVAS SERNAQUE,  MONICA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'RIVASPLATA MALCA,  GIOVANA AYDEE' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'RIVERA MOGOLLON,  ILIANA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'RIVERA RIVERA,  GIOVANA ISABEL PATRICIA' Then 'LIMA 2' " +
                                                        "When Asesor = 'RIVERA SILVA,  JUAN DIEGO' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'RIVERA VASQUEZ,  INES STEFANY' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'RIVERA VILELA,  MILAGROS DEL CARMEN' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'RIVERA VILELA, MILAGROS DEL CARMEN' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'ROBLES GABANCHO DE CARRASCO,  YISELA CARMELA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ROBLES NORIEGA,  ZOILA ESPERANZA' Then 'LIMA 1' " +
                                                        "When Asesor = 'RODRIGUEZ GAMARRA,  SONIA ESTHER' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'RODRIGUEZ GARCIA,  MONICA CLOTILDE' Then 'TRUJILLO' " +
                                                        "When Asesor = 'RODRIGUEZ GOMEZ,  JUAN MIGUEL' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'RODRIGUEZ RIVERA,  GLADYS ELIZABETH' Then 'LIMA 3' " +
                                                        "When asesor = 'RODRIGUEZ SANCHEZ DE SALVATIERRA, BEATRIZ D FIORELLA' Then 'LIMA 1' " +
                                                        "When Asesor = 'RODRIGUEZ UBILLUS,  RAFAELA LORENA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ROJAS ALCOCER,  NERY JUANA' Then 'ICA' " +
                                                        "When Asesor = 'ROJAS ALVA,  RUBEN RUBEN' Then 'CHICLAYO' " +
                                                        "When Asesor = 'ROJAS CORDOVA,  VILMA LOURDES' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'ROJAS DIAZ,  CAROLINA ELIZABETH' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'ROJAS HUARINGA,  ANALY MILAGROS' Then 'LIMA 2' " +
                                                        "When Asesor = 'ROJAS RIOS,  ANA MARIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ROMACO S.A.C. CORREDORES DE SEGURO' Then 'SAN BORJA' " +
                                                        "When Asesor = 'ROMERO CHAMORRO,  LIZET DOMENICA' Then 'HUANCAYO - CANAL EV' " +
                                                        "When Asesor = 'RONCALLA LUQUE,  CAMILA DEL CARMEN' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'RONDON ARCE,  JONATHAN FRANK' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ROSAS OLIVEROS,  ZARELLA LIZBETH' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'RUIZ BARDALES,  ELSA JUDITH' Then 'LIMA 1' " +
                                                        "When Asesor = 'RUIZ CUADROS,  CAROL VANESSA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'RUIZ MERA,  STEPHANIE MARICRUZ' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'RUMICHE LAZO,  ERICK JOE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SAAVEDRA DOMINGUEZ,  ROCIO NEILYTH' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'SAAVEDRA RUIZ,VILMA YOLANDA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'SAENZ CALZADO,  RICHARD WILLIAMS' Then 'LIMA 2' " +
                                                        "When Asesor = 'SALAS MARTINEZ,  LUISA CAROLINA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SALAS MONTOYA,  MARIA EUGENIA JACQUELINE' Then 'AREQUIPA - CANAL EV' " +
                                                        "When Asesor = 'SALAS ROMERO,MARY CAROL' Then 'SAN BORJA' " +
                                                        "When Asesor = 'SALAS VELAZCO,  ELIANA SILVIA' Then 'AREQUIPA - CANAL EV' " +
                                                        "When Asesor = 'SALAZAR GUERRERO,  LENIZ LUPITA' Then 'LIMA 3' " +
                                                        "When Asesor = 'SALAZAR GUEVARA,  SOLDVER CLARK' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SALAZAR PARRA,  CARLOS ALFREDO' Then 'LIMA 3' " +
                                                        "When Asesor = 'SALAZAR VARGAS DE TOMATIS,  LILIANA' Then 'PIURA' " +
                                                        "When Asesor = 'SALINAS HUATANGARE,  JUANA LUCILA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SALINAS PAREDES,  ROSA EDITH' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SALINAS TODOROVICH,  ALEJANDRO XAVIER' Then 'LIMA 1' " +
                                                        "When Asesor = 'SALIRROSAS ESPEJO,  ANA MARIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'SALVATIERRA MENDOZA,  ANA MARIA' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'SANCHEZ BARBOZA,  YEISY SEBASTIANA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SANCHEZ CARRANZA,  DANILO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SANCHEZ CASTAÑEDA,  YANINA ANABEL' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'SANCHEZ CHAVEZ,  ROSARIO DE LAS MERCEDES' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SANCHEZ LLENQUE,  KARLA ISABEL' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SANCHEZ POLO,  YSELA JANET' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SANCHEZ POSSO,  GERSON EUSEBIO' Then 'LIMA 3' " +
                                                        "When Asesor = 'SÁNCHEZ RODRIGUEZ,  RUDI ESTEFANI' Then 'LIMA 2' " +
                                                        "When Asesor = 'SANCHEZ RODULFO,  JENIFER ROSALYN' Then 'LIMA 3' " +
                                                        "When Asesor = 'SANCHEZ SOJO, SYBIL MARIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SANDOVAL FLORES,  PEDRO WILBERTO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SANGAMA WEILL,  JACKELINE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SANTA CRUZ ALEJANDRIA,  ANATALY' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'SANTA CRUZ CENTURION,  ESTEPHANY TERESA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'SANTA CRUZ POLUCHE,  RONAL GUSTAVO' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SANTISTEBAN GONZALES,  JULIO CESAR' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SANTTI QUEREVALU,  MARIA DE LOURDES' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SARMIENTO ACUÑA,  LADY MARYURI MARTINA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SARMIENTO MENDOZA,  MARGARITA MILAGROS' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SCC-ELITE VIRTUAL' Then 'SAN BORJA' " +
                                                        "When Asesor = 'SCHULZ CACERES,  KAREM DEL SOCORRO GENOVEVA' Then 'TRUJILLO' " +
                                                        "When Asesor = 'SEGOVIA HUAMBACHANO,  MARIA ROSA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SEGURA SONO,  IRENE DEL ROCIO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SEJO SUAREZ,  LILIANA CRISTINA' Then 'LIMA 1' " +
                                                        "When Asesor = 'SEVILLA YATACO,  SILVIA' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'SIANCAS VIERA,  SOCORRO NOEMI' Then 'LIMA 3' " +
                                                        "When Asesor = 'SIFUENTES OLAECHEA CORREDORES DE SEGUROS S.A.C.' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'SIME CORREA,  HECTOR SANTIAGO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SIRHUA BOLO,  MICHELLE' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SIU IGLESIAS,DORA ' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'SOLANO LATORRE,  EMANUEL ERNESTO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SOLIS GARCIA,  EVELYN JASMILETH' Then 'LIMA 3' " +
                                                        "When Asesor = 'SOLIS MORALES,  EVA LUCERO' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'SOSA ARBAIZA,  CYNTHIA VANESSA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'SOSA PANTA, LUIS REYNALDO' Then 'PIURA' " +
                                                        "When Asesor = 'SOTO QUIJANO,  CECILIA TERESA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'SUCLUPE CORONEL,  LUIS MANUEL' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'SUCUPLE SANTISTEBAN,  JOSE ALEX' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'TAKAMURA VILLAR,  PRISCILLA' Then 'LIMA 3' " +
                                                        "When Asesor = 'TALLEDO PILCO,  MARISELA BESSY' Then 'ICA' " +
                                                        "When Asesor = 'TAPIA ANDRES,  JUAN CARLOS' Then 'LIMA 2' " +
                                                        "When Asesor = 'TAPIA MAICELO,  ANA DEISSY' Then 'LIMA 1' " +
                                                        "When Asesor = 'TAVERA GUTIERREZ,  ZOILA IVETTE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'TEAS SOLARI,  GIULIANA MARISA' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'TELLO GONZALES,  ZULEMA ENRIQUETA' Then 'LIMA 1' " +
                                                        "When Asesor = 'TELLO GUEVARA,  MONICA MILAGROS' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'TELLO VARGAS,  JESSICA CATHERINA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'TERAN CRUZ,  MARIA LAURA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'TERRONES CAMPOS,  JOSE LUIS' Then 'LIMA 3' " +
                                                        "When Asesor = 'THOMAS MIRANDA,  SUE ELLEN' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'TICONA CASTILLO,  EVA ANGELICA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'TITANIO CORREDORES DE SEGUROS S.A.C.' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'TOLEDO ZAPANA DE CAMPOS,  ELIZABETH VICENTA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'TORALES PEREA,  JAVIER GONZALO' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'TORI POICON,  ZULEIKA LOURDES' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'TORO SOTOMAYOR,  CARMEN CECILIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'TORPOCO MOYA,  MARGOT' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'TORRES AGURTO,  ROXANA MERCEDES' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'TORRES DELGADO,  TALIA IBET' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'TORRES FANOLA,  GINA VANESSA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'TORRES FARFAN,  ROSARIO HAYDEE' Then 'LIMA 1' " +
                                                        "When Asesor = 'TORRES FRANCO,  LIDIA ELIZABETH' Then 'CHICLAYO' " +
                                                        "When Asesor = 'TORRES GARCIA,  ROSA LIZBELL' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'TORRES KOC,  HELEN LILIANA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'TORRES LABAN DE RENTERIA,  JYNNEE MARISOL' Then 'PIURA' " +
                                                        "When Asesor = 'TORRES MENDOZA,  HORTENCIA MARIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'TORRES MILLAN,  NAILETH NAZARETH' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'TORRES REATEGUI,  JACQUELINE LESLY' Then 'LIMA 3' " +
                                                        "When Asesor = 'TORRES TELLO,  CINTHIA KATHERYNE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'TORRES VILCA,  DENIS JOAQUIN' Then 'LIMA 1' " +
                                                        "When Asesor = 'TUCCIO VALVERDE,  JOSE ANTONIO' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'TUESTA ABENSUR,  RINA PAOLA VANESSA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'UÑURUCO PANTI,  ALICIA' Then 'LIMA 1' " +
                                                        "When Asesor = 'UBIDIA MUNIVE,  ELISA RUTH' Then 'SAN BORJA' " +
                                                        "When Asesor = 'UGAZ BASTANTE,  NICOLL NISI' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'URBINA GUARNIZO, TUCKY ASTERIA' Then 'PIURA' " +
                                                        "When Asesor = 'URBIOLA AYQUIPA,  ROSA CONSUELO' Then 'LIMA 1' " +
                                                        "When Asesor = 'URBISAGASTEGUI VARGAS,  CAROL JAZMIN' Then 'LIMA 2' " +
                                                        "When Asesor = 'URDANIGA QUIJANDRIA,  ANGELICA JACKELINE' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'UREÑA VILLAZANTE,  NEIL GREGORIO' Then 'PIURA' " +
                                                        "When Asesor = 'URRUNAGA TORRES,  ROSA ADRIANA' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'URTEAGA PENEDO,  KATIA MERCEDES' Then 'LIMA 2' " +
                                                        "When Asesor = 'VALDERRAMA ELLIS,  ZERENITH ROSARIO' Then 'AREQUIPA - CANAL EV' " +
                                                        "When Asesor = 'VALDIVIA CHACON,  ROSA MARLENE' Then 'AREQUIPA' " +
                                                        "When Asesor = 'VALDIVIEZO MOGOLLON,  ROSA GIULLIANA' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'VALDIVIEZO NUÑEZ,  TANIA CLEOPATRA' Then 'PIURA' " +
                                                        "When Asesor = 'VALDIVIEZO NUÑEZ,  TERESA DEL PILAR' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'VALDIVIEZO ROBLEDO,  HELEN JENNIFER' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'VALENCIA VASQUEZ,  KATTYUSCH MARIA LIBIA' Then 'LIMA 3' " +
                                                        "When Asesor = 'VALENZUELA ACOSTA,  ROSA AMELIA EUCARIS' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'VALVERDE ARANA,  CRISTIAN ALEXANDER' Then 'LIMA 2' " +
                                                        "When Asesor = 'VARELA DIAZ,  DONNA PATRICIA CECILIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'VASQUEZ LAZARTE,  SANDRA MARGOT' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'VASQUEZ LEON,  RENZO JHOVANNY' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'VASQUEZ PALOMINO,  LUIS MIGUEL YEMEN' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'VASQUEZ POMEZ,  CARMEN ISABEL' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'VEGA GAMARRA DE QUEIROLO,  NINA DEL PILAR' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'VEGA GAMARRA,  NINA DEL PILAR' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'VEGA NUÑEZ,  GLORY ANABELLA DE LOS MILAGROS' Then 'PIURA - CANAL EV' " +
                                                        "When Asesor = 'VELA FLORES,  MARIA GIULANNA' Then 'LIMA 2' " +
                                                        "When Asesor = 'VELARDE CONSULTORES FINANCIEROS S.A.C.' Then 'CHICLAYO' " +
                                                        "When Asesor = 'VELASQUE SAAVEDRA,  RENATO' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'VELEZ-VAUPEL & ASOCIADOS S.A' Then 'SAN BORJA' " +
                                                        "When Asesor = 'VELIZ PERLECHE,  NATALIA GRISELDA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'VENTOCILLA ZAVALA,  SARA ZULEMA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'VENTURA ARANGO,  SILVIA ISABEL' Then 'LIMA 1' " +
                                                        "When Asesor = 'VERA MENDOZA,  JUANA CECILIA' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'VERGARA ALARCON,  JUDITH BETTY' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'VERGARA GOMEZ,  GIOVANA GLORIA' Then 'ICA - CANAL EV' " +
                                                        "When Asesor = 'VIDAL NAVARRETE,  JOSE LUIS' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'VIDALON ADRIANZEN,  DEBORAH MARIA DEL CARMEN' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'VIDAURRE AGUILAR DE ROSSITER,  KAREN PATRICIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'VIGIL ARGUEDAS,  GEORGINA LUZ' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'VIGO CASTELLANOS,  JENNIFER DEL ROSARIO' Then 'LIMA 3' " +
                                                        "When Asesor = 'VILA ALEJOS,  LESLY ANGELY' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'VILCA SANTOS,  LOURDES ALICIA' Then 'LIMA 1' " +
                                                        "When Asesor = 'VILELA AGUINAGA,  JORGE LUIS' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'VILLANUEVA ENRIQUEZ,  CLEYDEE MARYLIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'VILLANUEVA SANCHEZ,  NEISSER YSSENIA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'VILLANZONA ACOSTA,  STEPHANIE BRIGITTE' Then 'LIMA 1' " +
                                                        "When Asesor = 'VILLARAN PACHAS,  JESSICA MARIA' Then 'LIMA 3' " +
                                                        "When Asesor = 'VILLEGAS ARENAS,  MARIA LELIANA' Then 'AREQUIPA' " +
                                                        "When Asesor = 'VILLEGAS ARENAS,MARIA LELIANA' Then 'AREQUIPA' " +
                                                        "When Asesor = 'WALDE RENTERIA,  LOURDES DEL PILAR' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'WILLIS ZOEGER,  JOSE EDUARDO' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'WONG MENDOZA,  MAX MIGUEL' Then 'CHICLAYO' " +
                                                        "When Asesor = 'XIQUES MEDINA,  YINAELY' Then 'CHICLAYO' " +
                                                        "When Asesor = 'YACTAYO FELIX,  PERCY DENNIS' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'YACUZZI RIVERA,  RAMON FRANCO' Then 'LIMA 2' " +
                                                        "When Asesor = 'YAMAGUCHI BODERO,  DARIO TADASHI' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'YARLEQUE RUIZ,  VERONICA CAROLINA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'YARLEQUE SULLON,  MARIA STEPHANY' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'YARROW RAFFO,  GIOVANNA ESTHER' Then 'MIRAFLORES' " +
                                                        "When Asesor = 'YEPEZ SANCHEZ,  CLAUDIA CAROLINA' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'YPANAQUE BALLONA,  MANUEL' Then 'TRUJILLO - CANAL EV' " +
                                                        "When Asesor = 'YRIGOIN FALLAQUE,  CAROLINA DEL ROCIO' Then 'CHICLAYO - CANAL EV' " +
                                                        "When Asesor = 'YUNGO QUISPE,  NORMA' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ZAMBRANO CHUNGA,  FIORELLA VIANNEY' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ZAMORA ARMESTAR,  MIRIAM NOHELY' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ZAPATA DEZA,  ANA MARIA' Then 'SAN BORJA' " +
                                                        "When Asesor = 'ZAVALA OLIVOS,  ROSA AURORA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ZAVALETA AQUINO,  LUIS ALBERTO' Then 'LIMA 1' " +
                                                        "When Asesor = 'ZEGARRA GONZALES,YESSIE LOURDES' Then 'AREQUIPA' " +
                                                        "When Asesor = 'ZEGARRA PERICHE,  KARINA PATRICIA' Then 'RENOVACIONES CA' " +
                                                        "When Asesor = 'ZEGARRA SANCHEZ,  KAREN LIZBHET' Then 'AUNA SALUD' " +
                                                        "When Asesor = 'ZEGARRA TEJADA,  ANA JULISSA' Then 'AREQUIPA' " +
                                                        "When Asesor = 'ZUAZO URIBE,  FERNANDO ALEXANDER' Then 'LIMA 3' " +
                                                        "else NULL end"))
                                                            
tf_df_bajas = filter_df_hb.select("Cod_Cliente","Cod_Grupo_Familiar","Periodo").distinct()

tf_df_bajas = tf_df_bajas.withColumn("Cod_GF",lpad(F.col("Cod_Grupo_Familiar"),10,"0000000000")) \
                         .withColumn("FechaBaja",F.concat(F.concat(F.concat(F.substring(F.col("Periodo"),1,4),F.lit("-")),F.substring(F.col("Periodo"),5,2)),F.lit("-01")).cast('date'))

join_df_ha = tf_df_altas.join(tf_df_bajas,(tf_df_altas.Cod_GF == tf_df_bajas.Cod_GF) & (tf_df_altas.cod_cliente == tf_df_bajas.Cod_Cliente) & (tf_df_bajas.FechaBaja > tf_df_altas.FechaAlta),"leftouter")

df_results_ha = join_df_ha.select(F.col("DES_CANAL"),F.col("DES_OFICINA_VENTA"),F.col("DES_GRUPO_VENDEDOR"),F.col("DES_SEDE"),F.col("des_programa"),F.col("rng_edad").alias("DES_RANGO_ETAREO"),F.col("perfil_bi").alias("DES_SEGMENTO"),F.col("tipo_pago"),F.col("DES_COORDINADOR"),F.col("asesor").alias("DES_ASESOR"),F.col("FechaAlta").alias("FECHA_ALTA"),F.col("FechaBaja").alias("FECHA_BAJA"))
 
tf_df_afil = tf_df_afil.select(F.col("CANAL_DIST"),F.col("DES_OFICINA_VENTA"),F.col("GRUPO_VENDEDOR"),F.col("DES_SEDE"),F.col("PROGRAMA"),F.col("DES_RANGO_ETAREO"),F.col("DES_SEGMENTO"),F.col("DES_TIPO_PAGO"),F.col("COORDINADOR").alias("DES_COORDINADOR"),F.col("NOMBRE_VENDEDOR").alias("DES_ASESOR"),F.col("FEC_AFIL_CONVERSION").alias("FECHA_ALTA"),F.col("FEC_EJEC_BAJA_AFIL").alias("FECHA_BAJA"))

tf_df_afil = tf_df_afil.union(df_results_ha)

transforms_DF = tf_df_afil.withColumn("FEC_ALTA",F.when(F.col("FECHA_ALTA").isNull(),to_date(F.lit("1900-01-01"),"yyyy-MM-dd")).otherwise(F.col("FECHA_ALTA"))) \
                             .withColumn("FEC_BAJA",F.when(F.col("FECHA_BAJA").isNull(),to_date(F.lit("1900-01-01"),"yyyy-MM-dd")).otherwise(F.col("FECHA_BAJA")))

transforms_DF = transforms_DF.withColumn("MES_ALTA",date_format(F.col("FEC_ALTA"),"yyyyMM")) \
                             .withColumn("MES_BAJA",date_format(F.col("FEC_BAJA"),"yyyyMM")) \
                             .withColumn("MES1",date_format(add_months(F.col("FEC_ALTA"), 1),"yyyyMM")) \
                             .withColumn("MES2",date_format(add_months(F.col("FEC_ALTA"), 2),"yyyyMM")) \
                             .withColumn("MES3",date_format(add_months(F.col("FEC_ALTA"), 3),"yyyyMM")) \
                             .withColumn("MES4",date_format(add_months(F.col("FEC_ALTA"), 4),"yyyyMM")) \
                             .withColumn("MES5",date_format(add_months(F.col("FEC_ALTA"), 5),"yyyyMM")) \
                             .withColumn("MES6",date_format(add_months(F.col("FEC_ALTA"), 6),"yyyyMM")) \
                             .withColumn("MES7",date_format(add_months(F.col("FEC_ALTA"), 7),"yyyyMM")) \
                             .withColumn("MES8",date_format(add_months(F.col("FEC_ALTA"), 8),"yyyyMM")) \
                             .withColumn("MES9",date_format(add_months(F.col("FEC_ALTA"), 9),"yyyyMM")) \
                             .withColumn("MES10",date_format(add_months(F.col("FEC_ALTA"), 10),"yyyyMM")) \
                             .withColumn("MES11",date_format(add_months(F.col("FEC_ALTA"), 11),"yyyyMM")) \
                             .withColumn("MES12",date_format(add_months(F.col("FEC_ALTA"), 12),"yyyyMM"))

transforms_DF=transforms_DF.withColumn("T_CAN_MES0",F.lit(1)) \
                           .withColumn("T_CAN_MES1",F.when(F.col("MES1") < anio_mes,F.when(F.col("MES_ALTA") != F.col("MES_BAJA"),F.when(F.col("MES1") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES2",F.when(F.col("MES2") < anio_mes,F.when(F.col("T_CAN_MES1") == 1,F.when(F.col("MES2") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES3",F.when(F.col("MES3") < anio_mes,F.when(F.col("T_CAN_MES2") == 1,F.when(F.col("MES3") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES4",F.when(F.col("MES4") < anio_mes,F.when(F.col("T_CAN_MES3") == 1,F.when(F.col("MES4") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES5",F.when(F.col("MES5") < anio_mes,F.when(F.col("T_CAN_MES4") == 1,F.when(F.col("MES5") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES6",F.when(F.col("MES6") < anio_mes,F.when(F.col("T_CAN_MES5") == 1,F.when(F.col("MES6") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES7",F.when(F.col("MES7") < anio_mes,F.when(F.col("T_CAN_MES6") == 1,F.when(F.col("MES7") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES8",F.when(F.col("MES8") < anio_mes,F.when(F.col("T_CAN_MES7") == 1,F.when(F.col("MES8") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES9",F.when(F.col("MES9") < anio_mes,F.when(F.col("T_CAN_MES8") == 1,F.when(F.col("MES9") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES10",F.when(F.col("MES10") < anio_mes,F.when(F.col("T_CAN_MES9") == 1,F.when(F.col("MES10") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES11",F.when(F.col("MES11") < anio_mes,F.when(F.col("T_CAN_MES10") == 1,F.when(F.col("MES11") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None))) \
                           .withColumn("T_CAN_MES12",F.when(F.col("MES12") < anio_mes,F.when(F.col("T_CAN_MES11") == 1,F.when(F.col("MES12") != F.col("MES_BAJA"),1).otherwise(0)).otherwise(0)).otherwise(F.lit(None)))

Group_DF = transforms_DF.select("DES_OFICINA_VENTA","GRUPO_VENDEDOR","PROGRAMA","DES_RANGO_ETAREO","DES_SEGMENTO","CANAL_DIST","DES_SEDE","DES_ASESOR","DES_TIPO_PAGO","DES_COORDINADOR","MES_ALTA","MES_BAJA","T_CAN_MES0","T_CAN_MES1","T_CAN_MES2","T_CAN_MES3","T_CAN_MES4","T_CAN_MES5","T_CAN_MES6","T_CAN_MES7","T_CAN_MES8","T_CAN_MES9","T_CAN_MES10","T_CAN_MES11","T_CAN_MES12")

Group_DF=Group_DF.groupBy("DES_OFICINA_VENTA","GRUPO_VENDEDOR","PROGRAMA","DES_RANGO_ETAREO","DES_SEGMENTO","CANAL_DIST","DES_SEDE","DES_ASESOR","DES_TIPO_PAGO","DES_COORDINADOR","MES_ALTA","MES_BAJA").agg(F.sum("T_CAN_MES0").alias("C_CAN_MES0"),F.sum("T_CAN_MES1").alias("C_CAN_MES1"),F.sum("T_CAN_MES2").alias("C_CAN_MES2"),F.sum("T_CAN_MES3").alias("C_CAN_MES3"),F.sum("T_CAN_MES4").alias("C_CAN_MES4"),F.sum("T_CAN_MES5").alias("C_CAN_MES5"),F.sum("T_CAN_MES6").alias("C_CAN_MES6"),F.sum("T_CAN_MES7").alias("C_CAN_MES7"),F.sum("T_CAN_MES8").alias("C_CAN_MES8"),F.sum("T_CAN_MES9").alias("C_CAN_MES9"),F.sum("T_CAN_MES10").alias("C_CAN_MES10"),F.sum("T_CAN_MES11").alias("C_CAN_MES11"),F.sum("T_CAN_MES12").alias("C_CAN_MES12"))

df_result_fields = Group_DF.select(F.col("CANAL_DIST").alias("DES_CANAL"),F.col("DES_OFICINA_VENTA"),F.col("GRUPO_VENDEDOR").alias("DES_GRUPO_VENDEDOR"),F.col("DES_SEDE"),F.col("PROGRAMA").alias("DES_PROGRAMA"),F.col("DES_RANGO_ETAREO"),F.col("DES_SEGMENTO"),F.col("DES_TIPO_PAGO"),F.col("DES_COORDINADOR"),F.col("DES_ASESOR"),F.col("MES_ALTA"),F.col("MES_BAJA"),F.col("C_CAN_MES0").cast('int').alias("CAN_MES0"),F.col("C_CAN_MES1").cast('int').alias("CAN_MES1"),F.col("C_CAN_MES2").cast('int').alias("CAN_MES2"),F.col("C_CAN_MES3").cast('int').alias("CAN_MES3"),F.col("C_CAN_MES4").cast('int').alias("CAN_MES4"),F.col("C_CAN_MES5").cast('int').alias("CAN_MES5"),F.col("C_CAN_MES6").cast('int').alias("CAN_MES6"),F.col("C_CAN_MES7").cast('int').alias("CAN_MES7"),F.col("C_CAN_MES8").cast('int').alias("CAN_MES8"),F.col("C_CAN_MES9").cast('int').alias("CAN_MES9"),F.col("C_CAN_MES10").cast('int').alias("CAN_MES10"),F.col("C_CAN_MES11").cast('int').alias("CAN_MES11"),F.col("C_CAN_MES12").cast('int').alias("CAN_MES12"),F.lit(F.current_date()).alias("FEC_CARGA"))

#guardado
df_result_fields.write.mode('overwrite') \
		        .format('parquet') \
		        .save('s3://auna-dlaprd-stage-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf_tablero_permanencia/')