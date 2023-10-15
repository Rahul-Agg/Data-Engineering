import os
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
import uuid
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from time import sleep
import json
import requests
# ps.set_option('compute.max_rows', None)
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.mysql:mysql-connector-j:8.0.33 pyspark-shell'

MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaToIcebergMiniIO") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0,com.mysql:mysql-connector-j:8.0.33')\
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")\
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.io-impl","org.apache.iceberg.hadoop.HadoopFileIO") \
    .config("spark.sql.catalog.iceberg.warehouse","s3a://iceberg-bucket/iceberg-warehouse")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY) \
    .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY) \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()


partition_function_mapping = {
    "months": "date", 
    "years":"date", 
    "days":"date", 
    "hours":"hour", 
    "bucket":"bucket"
}

mysql_to_iceberg_spark_typemapping = {
    "longtext": "string",
    "bigint": "long",
    "boolean": "integer",
    "int": "integer",
    "bytes": "integer",
    "int16": "integer",
    "int32": "integer",
    "int64": "long",
    "float32": "float",
    "float64": "double",
    "double":"double",
    "string": "string",
    "enum": "string",
    "text": "string",
    "mediumtext": "string",
    "json": "string",
    "datetime": "timestamp",
    "date":"date",
    "set": "string",
    "char": "string",
    "varbinary": "string",
    "tinyint": "integer",
    "blob": "string",
    "polygon": "string",
    "time": "long",
    "multipolygon": "string",
    "smallint": "integer",
    "geometry": "string",
    "linestring": "string",
    "multipoint": "string",
    "multilinestring": "string",
    "geometrycollection": "string",
    "decimal":"decimal(38,2)",
    "varchar":"string",
    "binary": "string",
    "point":"string",
    "timestamp":"timestamp"
    }



def get_iceberg_schema_df(catalog,schema,table):
    df = spark.sql(f'Describe  {catalog}.{schema}.{table}')
    df = df.filter((~df.col_name.rlike('Part .*')))
    df = df.filter((~df.col_name.rlike('#.*')))
    df = df.filter((df.col_name != '' ))
    return df

def add_deleted_column_with_null(schema_df, data_df):
    df = schema_df.collect()
    for row in df:
        col_name = row['col_name']
        if col_name not in data_df.columns:
            data_df= data_df.withColumn(col_name,lit(None))  
    return data_df

def get_function_and_column_name_from_partition_spec(value):
    function = None
    if "(" in value:
        function = value[0:value.find("(")].strip()
    if "months" == function or "days" == function or "years" == function or "hours" == function:
        return partition_function_mapping.get(function),value[value.find("(")+1:value.find(")")].strip() 
    elif "bucket" == function:
        return partition_function_mapping.get(function), value[value.find("(")+1:value.find(")")].split(",")[1].strip()
    return None,None

def get_partition_key_and_function(table_name):
    result = []
    df =spark.sql(f"describe {catalog}.{db}.{table_name}")
    df = df.filter(df.col_name.rlike('Part .*')).sort("col_name").collect()
    if len(df) == 0:
        return None
    for row in df:
        function,column_name = get_function_and_column_name_from_partition_spec(row["data_type"])
        if column_name:
            result.append([function,column_name])
    return result

def create_table(table_name, columns, partition_column, key):
    properties_query= f"""
                TBLPROPERTIES (
                    'format-version'='2',
                    'write.distribution-mode'='hash',
                    "write.parquet.compression-codec"=  "snappy",
                    "target-file-size-bytes"= "536870912",
                    "write.delete.mode"="copy-on-write",
                    "write.update.mode"= "copy-on-write",
                    "write.merge.mode"="copy-on-write",
                    "write.metadata.metrics.default"="full",
                    "write.spark.fanout.enabled"="true"
                )
        """
    table_query = f"""CREATE TABLE {catalog}.{db}.{table_name} ("""
    for name, type in columns.items():
        table_query += name + " " + type + ","
    table_query = table_query[:-1] + """) USING ICEBERG"""
    if partition_column:
        partition_query = f""" PARTITIONED BY (MONTHS({partition_column})) """
    else:
        partition_query = f""" PARTITIONED BY (__rds_id) """
    spark.sql(table_query + partition_query + properties_query)

def schema_setter(df, schema,key):
    typeMapping = {
            "boolean": BooleanType(),
            "bytes": StringType(),
            "int16": ShortType(),
            "int32": IntegerType(),
            "int64": LongType(),
            "float32": FloatType(),
            "float": FloatType(),
            "float64": DoubleType(),
            "double": DoubleType(),
            "string": StringType(),
            "io.debezium.time.zonedtimestamp": StringType(),
            "io.debezium.time.date": LongType(),
            "io.debezium.time.year": IntegerType(),
            "io.debezium.time.timestamp": LongType(),
            "io.debezium.time.microtime": LongType(),
            "io.debezium.time.microtimestamp": LongType(),
            "io.debezium.data.geometry.geometry": StringType(),
            "binary": StringType()
    }
    typeMappingForPartition = {
            "boolean": "boolean",
            "bytes": "string",
            "int16": "integer",
            "int32": "integer",
            "int64": "long",
            "float32": "float",
            "float": "float",
            "float64": "double",
            "double": "double",
            "string": "string",
            "io.debezium.time.zonedtimestamp": "timestamp",
            "io.debezium.time.date": "date",
            "io.debezium.time.year": "integer",
            "io.debezium.time.timestamp": "timestamp",
            "io.debezium.time.microtime": "long",
            "io.debezium.time.microtimestamp": "timestamp",
            "io.debezium.data.geometry.geometry": "string",
            "io.debezium.data.json": "string",
            "io.debezium.data.enum": "string",
            "io.debezium.data.enumset": "string",
            "binary": "string"
    }
    arr = []
    schema_info = {}
    partition_column = None
    if schema != None:
        if 'fields' in schema and len(schema['fields']) > 0:
            for column_info in schema['fields']:
                if 'name' in column_info:
                    dtype =  typeMapping.get(column_info['name'].lower(), StringType())
                    dtype_partition =  typeMappingForPartition.get(column_info['name'].lower(), "string")
                else:
                    dtype =  typeMapping.get(column_info['type'].lower(), StringType())
                    dtype_partition =  typeMappingForPartition.get(column_info['type'].lower(), "string")
                if key and key==column_info['field']:
                    arr.append(StructField(column_info['field'], dtype, False))
                else:
                    arr.append(StructField(column_info['field'], dtype, True))
                if column_info['field'] != '__deleted':
                    schema_info[column_info['field']] = dtype_partition
                    if partition_column == None and column_info['optional'] == False and "create" in column_info['field'] and ("date" == dtype_partition or "timestamp" == dtype_partition):
                        partition_column = column_info['field']
    value_schema = StructType([StructField("schema", StringType(), True), StructField("payload", StringType(), True)])
    df = df.withColumn("value", from_json("value", value_schema)).withColumn("value", from_json("value.payload", StructType(arr))).withColumn("__op", col("__op").cast(StringType())).withColumn('__ts_ms', to_timestamp(from_unixtime(col('__ts_ms')/ 1000))).select(col('value.*'), col('__op'), col('__db'), col('__ts_ms'), col('__topic'))
    arr.append(StructField('__op', StringType(), False))
    arr.append(StructField('__db', StringType(), False))
    arr.append(StructField('__dp_update_ts', TimestampType(), False))
    arr.append(StructField('__topic', StringType(), False))
    schema_info['__dp_update_ts'] = "timestamp"
    df = spark.createDataFrame(df.rdd, StructType(arr))
    if schema != None:
        if 'fields' in schema and len(schema['fields']) > 0:
            for column_info in schema['fields']:
                if 'name' in column_info:
                    if column_info['name'].lower() == "io.debezium.time.zonedtimestamp":
                        df = df.withColumn(column_info['field'], to_timestamp(date_format(column_info['field'], "yyyy-MM-dd HH:mm:ss")))
                    elif column_info['name'].lower() == "io.debezium.time.date":
                        df = df.withColumn(column_info['field'], to_date(from_unixtime(col(column_info['field'])* 86400)))
                    elif column_info['name'].lower() == "io.debezium.time.timestamp":
                        df = df.withColumn(column_info['field'], to_timestamp(from_unixtime(col(column_info['field'])/ 1000)))
                    elif column_info['name'].lower() == "io.debezium.time.microtimestamp":
                        df = df.withColumn(column_info['field'], to_timestamp(from_unixtime(col(column_info['field'])/ 1000000)))
                    elif column_info['name'].lower() == "org.apache.kafka.connect.data.decimal":
                        # df = df.withColumn(column_info['field'], binary_decimal(col(column_info['field'])).cast(DecimalType()))
                        df = df.withColumn(column_info['field'], col(column_info['field'])).cast(DecimalType())
                    elif column_info['name'].lower() == "io.debezium.time.microtime":
                        df = df.withColumn(column_info['field'], to_timestamp(from_unixtime(col(column_info['field'])/ 1000000)))
#                 elif column_info['type'].lower() == "boolean":
#                     df = df.withColumn(column_info['field'], when(col(column_info['field']) == True,1).otherwise(0).cast(IntegerType()))
    return df, schema_info, partition_column

def merge_to_table(df, table, columns, key, partition_key, partition_function):
    df._jdf.sparkSession().sql(f"refresh {table}")
    # columns.remove("__op")
    update_list = []
    column_list = []
    column_list_s = []
    for x in columns:
        update_list.append("t."+x+"=s."+x)
        column_list.append(x)
        column_list_s.append("s."+x)
    column_list = ', '.join(column_list)
    column_list_s = ', '.join(column_list_s)
    update_list = ', '.join(update_list)
    temp_table_name= "update{}".format(str(uuid.uuid4()).replace('-',''))
    if partition_key is not None:
        df = df.orderBy(partition_key)
        if partition_key == key:
            merge_query = """MERGE INTO """+catalog+"."+db+"."+table+""" t USING (select * from """+ temp_table_name +""") s 
                                            on """ + " t.__rds_id = s.__rds_id and t.__tenant_id = s.__tenant_id and t."""+key+""" = s."""+key
        elif partition_key:
            merge_query = """MERGE INTO """+catalog+"."+db+"."+table+""" t USING (select * from """+  temp_table_name +""") s 
                                                on """+partition_function + "(t."+partition_key+")"+f""" = {partition_function}( s."""+partition_key+""") and t.__rds_id = s.__rds_id and t.__tenant_id = s.__tenant_id and t."""+key+""" = s."""+key
        else:
            merge_query = """MERGE INTO """+catalog+"."+db+"."+table+""" t USING (select * from """+  temp_table_name +""") s 
                                                on """ + " t.__rds_id = s.__rds_id and t.__tenant_id = s.__tenant_id and t."""+key+""" = s."""+key
    elif partition_key is None and key is not None:
        merge_query = """MERGE INTO """+catalog+"."+db+"."+table+""" t USING (select * from """+ temp_table_name +""") s 
                                            on t."""+key+""" = s."""+key + " and t.__rds_id = s.__rds_id and t.__tenant_id = s.__tenant_id"
    merge_query += """
                                            WHEN MATCHED AND s.__op = 'd' THEN DELETE
                                            WHEN MATCHED AND (s.__op = 'u' or s.__op = 'c' or s.__op = 'r')  THEN UPDATE SET """+update_list+"""
                                            WHEN NOT MATCHED AND NOT s.__op = 'd' THEN INSERT ("""+column_list+""") VALUES ("""+column_list_s+""")"""
    if key is None:
        merge_query = f"""INSERT INTO {table} ("""+column_list+""") VALUES ("""+column_list_s+""")"""
    
    exception = None
    df.show()
    print(merge_query)
    for x in range(10): 
        try:
            df.createOrReplaceTempView(temp_table_name)
            df._jdf.sparkSession().sql(merge_query)
            spark.catalog.dropTempView(temp_table_name)
            # print(f"completed merge for table {db}.{table}")
            return
        except Exception as e:
            df._jdf.sparkSession().sql(f"refresh {table}")
            exception = e
            sleep(10)
    raise Exception(table + ' db: ' + db  + ' ' +str(exception))

def add_multitenant_columns(df, columns):
    df = df.withColumn("__tenant_id", regexp_extract(col('__db'), '(\\d+)', 1).cast(IntegerType()))
    df = df.withColumn("__rds_id", regexp_extract(col('__topic'), '(\\d+)', 1).cast(IntegerType()))
    df = df.withColumn("__tenant_id", expr("IF(__tenant_id is null, 0, __tenant_id)"))
    columns['__tenant_id'] = "integer"
    columns['__rds_id'] = "integer"
    columns['__op'] = "string"
    columns['__dp_update_ts'] = 'timestamp'
    return df, columns

def add_columns(db,name,cols,df):
    existing_columns = set(spark.sql(f"describe {catalog}.{db}.{name}").select('col_name').rdd.flatMap(lambda x: x).collect())
    for col in cols:
        if col.lower() not in existing_columns:
            col_type = dict(df.dtypes)[col]
            spark.sql("ALTER TABLE "+catalog+"."+db+"."+name+" ADD COLUMN "+col+" "+col_type)

def process_for_table(df, table):
    current_date_time_df = spark.sql("SELECT current_timestamp() as current_time").select(date_format("current_time", "dd HH:mm:ss"))
    current_time_value = current_date_time_df.first()[0]
    print(f""" Dataframe processing start for - {table} at {current_time_value} """)
    data = df.filter(df.__table == table)
    if df.isEmpty():
        return
    try:
        key = list(json.loads(data.select('key').tail(1)[0][0]).keys())[0]
    except Exception:
        key = None
    # partitions = []
    data = data.pandas_api()
    data= data.sort_values(by="__ts_ms")
    if key:
        data.drop_duplicates(subset=['key', '__topic'], keep='last', inplace=True)
    data = data.to_spark().sort("__ts_ms")
    schema = json.loads(data.select('value').tail(1)[0][0])['schema']
    data, tbl_cols, partition_column = schema_setter(data, schema, key)
    print("after schema",tbl_cols)
    data, tbl_cols = add_multitenant_columns(data, tbl_cols)
    print("after multi",tbl_cols)
    final_df= data.drop('__deleted').drop('__ts_ms').withColumn('__dp_update_ts',lit(current_timestamp()))
    if table in table_list:
        schema_df = get_iceberg_schema_df(catalog,db,table)
        final_df = add_deleted_column_with_null(schema_df, final_df)
    final_df= final_df.drop('__deleted').drop('__db').drop('__topic')
    for column in final_df.columns:
        # For handling boolean in glaucus
        if dict(final_df.dtypes)[column] == 'boolean' or dict(final_df.dtypes)[column] == 'smallint':
            final_df = final_df.withColumn(column, col(column).cast("int")) 
    if table not in table_list :
        create_table(table, tbl_cols, partition_column, key)
    else:
        add_columns(db,table,tbl_cols,final_df)
    columns = final_df.columns
    result= get_partition_key_and_function(table)
    partition_function= None
    partition_key= None
    if result is not None:
        partition_key = result[0][1]
        partition_function= result[0][0]

    current_date_time_df = spark.sql("SELECT current_timestamp() as current_time").select(date_format("current_time", "dd HH:mm:ss"))
    current_time_value = current_date_time_df.first()[0]
    
    print(f""" Dataframe processing end for - {table} at {current_time_value} """)
    print(f""" Dataframe merging start for - {table} at {current_time_value} """)
    merge_to_table(final_df, table, columns, key, partition_key, partition_function)
    # execute_compaction(db, table)
    print(f""" Dataframe merging end for - {table} at {current_time_value()} """)

    # print(f"Completed job for table {table}")


def read_from_kafka(subscribe_pattern):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS)) \
        .option("includeHeaders", "true") \
        .option("subscribePattern", f'{subscribe_pattern}$') \
        .option("startingOffsets", "earliest") \
        .option("kafka.fetch.max.wait.ms", "100000") \
        .option("failOnDataLoss", "false") \
        .load()
    return df


def write_to_sink(df, checkpoint_location):
    df \
        .withColumn("key", col("key").cast(StringType())) \
        .withColumn("value", col("value").cast(StringType())) \
        .withColumn("__table", expr("headers[0].value").cast(StringType())) \
        .withColumn("__op", expr("headers[1].value").cast(StringType())) \
        .withColumn("__ts_ms", expr("headers[2].value").cast(StringType()).cast(LongType())) \
        .withColumn("__db", expr("headers[3].value").cast(StringType())) \
        .withColumn("__topic", col("topic").cast(StringType())) \
        .selectExpr("key", "value", "__op", "__table", "__db", "__ts_ms", "__topic") \
        .writeStream \
        .option("checkpointLocation", checkpoint_location) \
        .foreachBatch(df_to_sink) \
        .trigger(once=True) \
        .start().awaitTermination()
    
def df_to_sink(df, batch_id=None):
    current_date_time_df = spark.sql("SELECT current_timestamp() as current_time").select(date_format("current_time", "dd HH:mm:ss"))
    current_time_value = current_date_time_df.first()[0]
    print(f""" Reading end from kafka for - {subscribe_pattern} at {current_time_value} """)
    df = df.cache()
    df = df.dropna(subset=['__op', 'value', '__table', '__db'])
    if df.isEmpty():
        return
    table = df.select('__table').collect()[0][0]
    
    process_for_table(df, table)

def run_job(subscribe_pattern, checkpoint_location):
    current_date_time_df = spark.sql("SELECT current_timestamp() as current_time").select(date_format("current_time", "dd HH:mm:ss"))
    current_time_value = current_date_time_df.first()[0]
    print(f""" Reading start from kafka for - {subscribe_pattern} at {current_time_value} """)
    stream = read_from_kafka(subscribe_pattern)
    write_to_sink(stream, checkpoint_location)

def execute_compaction(schema, table, full=False):
    print(f'Optimization for {schema}.{table} started.')
    with engine.connect() as cur:
        try:
            cur.exec_driver_sql(
                    f"""ALTER TABLE {CATALOG}.{schema}.{table} EXECUTE optimize(file_size_threshold => '512MB')""")
            cur.commit()
            if full:
                cur.exec_driver_sql(
                    f"""ALTER TABLE {CATALOG}.{schema}.{table} EXECUTE expire_snapshots(retention_threshold => '{RETENTION}')""")
                cur.commit()
                cur.exec_driver_sql(
                    f"""ALTER TABLE {CATALOG}.{schema}.{table} EXECUTE remove_orphan_files(retention_threshold  => '{RETENTION}')""")
                cur.commit()
        except Exception as e:
            print(f"Optimization failed for {schema}.{table} with error: ", e)
        finally:
            print(f'Optimization for {schema}.{table} done.')
            return None

if __name__ == '__main__':

    # ids = ['2','3','1']
    ids = ['1']
    patterns = ['oms']
    db = 'glaucus_oms'
    BOOTSTRAP_SERVERS= ['localhost:9092']
    LAKE_PATH = "s3a://iceberg-bucket/iceberg-warehouse/"
    user="root"
    password="debezium"
    catalog = "iceberg"
    db = "glaucus_oms"
    for id_number in ids:
        for pattern in patterns:

            SUBSCRIBE_PATTERN = f'source_glaucus{id_number}.{pattern}[\\d]*'
            db = f"glaucus_{pattern}"
            CHECKPOINT_LOCATION = LAKE_PATH + "kafka-checkpoints/" + db + id_number

            spark.sql(f"""CREATE DATABASE IF NOT EXISTS iceberg.{db}""")
            current_date_time_df = spark.sql("SELECT current_timestamp() as current_time").select(date_format("current_time", "dd HH:mm:ss"))
            current_time_value = current_date_time_df.first()[0]
            print(f"""***** Started Spark Job for PATTERN - {SUBSCRIBE_PATTERN} at {current_time_value} *****""")
            result = requests.get(f"{SUBSCRIBE_PATTERN}")
            topics = []
            for topic in result.json()['topics']:
                topics.append(topic['name'])
            subscribe_pattern = '' + SUBSCRIBE_PATTERN
            checkpoint_location =  '' + CHECKPOINT_LOCATION
            # table_list = set(spark.sql("show tables from "+ catalog + "." +db).select('tableName').rdd.flatMap(lambda x: x).collect())
            table_list = ['sales']
            topic_dict = {}
            dtopic_dict = {}
            for topic in topics:
                subscribe_pattern = topic
                checkpoint_location = CHECKPOINT_LOCATION +  "/" + topic
                topic_dict[subscribe_pattern] = checkpoint_location
                dtopic = topic.strip().split('.')[1]
                if dtopic in dtopic_dict:
                    dtopic_dict[dtopic].append(topic)
                else:
                    dtopic_dict[dtopic] = [topic]
        
            with ThreadPoolExecutor(max_workers=40) as executor:
                result_futures = [executor.submit(run_job, topic, loc) for topic,loc in topic_dict.items()]
                wait(result_futures)
                for future in as_completed(result_futures):
                    print(future.result())
            sleep(10)            
            current_date_time_df = spark.sql("SELECT current_timestamp() as current_time").select(date_format("current_time", "dd HH:mm:ss"))
            current_time_value = current_date_time_df.first()[0]
            print(f""" ***** Finished Spark Job for PATTERN - {SUBSCRIBE_PATTERN} at {current_time_value} ***** """)
            