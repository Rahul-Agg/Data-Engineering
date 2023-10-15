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
    "text": "string"
}
def add_columns(db,name,cols,df):
    existing_columns = set(spark.sql(f"describe {catalog}.{sink_db}.{name}").select('col_name').rdd.flatMap(lambda x: x).collect())
    for col in cols:
        if col.lower() not in existing_columns:
            col_type = mysql_to_iceberg_spark_typemapping.get(get_column_info(db,name, col).collect()[0]['DATA_TYPE'].lower(),"string")
            spark.sql("ALTER TABLE "+catalog+"."+sink_db+"."+name+" ADD COLUMN "+col+" "+col_type)
            
    for col in existing_columns:
        if col not in cols and (not col.startswith('#')) and (col != '__rds_id' and col != '__tenant_id' and col != '__ts_ms'):
#             dtype = spark.sql(f"describe {catalog}.{sink_db}.{name}")
#             dtype = .filter(dtype.col_name == col).select('data_type').rdd.flatMap(lambda x: x).collect()[0]
            df = df.withColumn(col, lit(None))

    return df
    
def get_primary_key(db, table):
    query = f"""select COLUMN_NAME, DATA_TYPE, COLUMN_KEY, IS_NULLABLE FROM information_schema.columns where TABLE_SCHEMA = '{db}' and TABLE_NAME = '{table}' and COLUMN_KEY='PRI' """
    
    schema=spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", url) \
        .option("query", query) \
        .option("user", user) \
        .option("password", password) \
        .option("fetchsize", 50) \
        .load()
    
    primary_key = schema.select('COLUMN_NAME').collect()
    if len(primary_key)>0:
        primary_key = primary_key[0][0]
    else:
        primary_key= None
    return primary_key

def read_from_mysql(table_name, key = None):
    
    if key != None:
        
        min_max = spark.read \
            .format("jdbc") \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("url", url) \
            .option("query", f"select min({key}), max({key}) from {table_name}") \
            .option("user", user) \
            .option("password", password) \
            .option("fetchsize", 10) \
            .load()
        
        minimum, maximum = min_max.collect()[0][0], min_max.collect()[0][1]
        
        if minimum == None or maximum == None:
            
            df = spark.read \
            .format("jdbc") \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("fetchsize", 10000) \
            .load()
            
            return df
        
        df = spark.read \
            .format("jdbc") \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("fetchsize", 10000) \
            .option("numPartitions", 5) \
            .option("partitionColumn", key) \
            .option("lowerBound", minimum) \
            .option("upperBound", maximum) \
            .load()
        
    else:
        
        df = spark.read \
            .format("jdbc") \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("fetchsize", 10000) \
            .load()
        
    return df
    
def get_table_info(db, table):
    query = f"""select COLUMN_NAME, DATA_TYPE, COLUMN_KEY, IS_NULLABLE, COLUMN_TYPE FROM information_schema.columns where TABLE_SCHEMA = '{db}' and TABLE_NAME = '{table}' """
    
    df=spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", url) \
        .option("query", query) \
        .option("user", user) \
        .option("password", password) \
        .option("numPartitions",5) \
        .option("fetchsize", 20) \
        .load()
    
#     df = df.withColumn("DATA_TYPE", when(df["COLUMN_TYPE"] == "tinyint(1)", "boolean").otherwise(df["DATA_TYPE"])).drop("COLUMN_TYPE")
    
    return df

def create_table(db, sink_db, table_name):
    table_query = f"""Create table if not exists {catalog}.{sink_db}.{table_name} ("""
    column_queries=[]
    partitioned_column = None
    primary_key= None
    schema = get_table_info(db,table_name).collect()
    if len(schema) ==0:
        raise Exception("Table Schema not found in source database")
    for row in schema:
        column_name = row["COLUMN_NAME"].strip().replace(" ", "_").lower()
        nullable = ""
        if partitioned_column == None and "create" in column_name and ("date" == row["DATA_TYPE"].lower() or "timestamp" == row["DATA_TYPE"].lower() or "datetime" ==row["DATA_TYPE"].lower()) and row["IS_NULLABLE"]== 'NO':
            partitioned_column = column_name
        if "PRI" == row["COLUMN_KEY"]:
            primary_key = column_name
#             nullable= "NOT NULL"     
        column_queries.append(f"""{column_name} {mysql_to_iceberg_spark_typemapping.get(row["DATA_TYPE"],"string")} {nullable}""".strip())
    column_queries.append('__tenant_id integer')
    column_queries.append('__rds_id integer')
    column_queries.append('__dp_update_ts timestamp')
    column_queries.append('__op string')  # added for __op
    partion_query= f""") {f"PARTITIONED BY (__tenant_id)"}
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
    table_query = table_query + ",".join(column_queries) + partion_query
    spark.sql(table_query)
def snapshot_to_iceberg_merged(rds_id, table_name, table_id, table_sink):
    # print(rds_id, table_name, table_id, table_sink)
    lake_table = table_sink
    db = table_name.strip().split(".")[0]
    print(f"Started for table {table_name}")        
    key = get_primary_key(db, table_sink)
    if table_sink in table_list:
        pass
    else:
        create_table(db, sink_db, table_sink)
       
    
    df = read_from_mysql(table_name, key)
    
    if df.isEmpty():
        print(f"Completed for EMPTY table {table_name}")
        return
    
    for column in df.columns:
        df = df.withColumnRenamed(column,column.strip().replace(" ", "_"))
            
    df = add_columns(db,table_sink,df.columns, df)
        
    if table_id == '':
        table_id = 0
        
    for column in df.columns:
        if dict(df.dtypes)[column] == 'boolean':
            df = df.withColumn(column, col(column).cast("int"))

    df.withColumn("__tenant_id", lit(int(table_id))) \
            .withColumn("__rds_id", lit(int(rds_id))) \
            .withColumn("__dp_update_ts", lit(current_timestamp()))\
            .withColumn("__op", lit('r')) \
            .repartition(500).writeTo(f"""{catalog}.{sink_db}.{table_sink}""").append()
    
    spark.sql(f"INSERT INTO iceberg.{sink_db}.insert_log{rds_id} VALUES ('{db}', '{table_sink}', '{sink_db}.{table_sink}')")
    
    print(f"Completed for table {table_name}")



url = "jdbc:mysql://localhost:3306"  # Use the IP of your Docker host if not running on localhost
ms_type='oms'
catalog='iceberg'
sink_db = 'glaucus_oms'
rds_id = 1
user='root'
password='debezium'
table_list = set(spark.sql("show tables in "+catalog+"."+sink_db).select('tableName').rdd.flatMap(lambda x: x).collect())
table_schemas = spark.read.format('jdbc')\
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", url) \
        .option('query',f""" select concat(table_schema, '.', table_name) as name,
        substr(table_schema, 4) as id ,table_name from information_schema.tables where table_schema REGEXP '{ms_type}[0-9]*' and table_name in ('sales','orders','orders_skus','sales_inventory') """)\
        .option("user", 'root') \
        .option("password", "debezium") \
        .load()
existing_tables = spark.sql(f"""SELECT CONCAT(schema, '.', name) as tableName FROM {catalog}.{sink_db}.insert_log{rds_id}""").select('tableName').rdd.flatMap(lambda x: x).collect()
# df.show()
table_schemas = table_schemas.filter(~col("name").isin(*existing_tables))
tables = table_schemas.rdd.map(lambda x: x[0]).collect()
temp_tables = []
table_schemas = table_schemas.filter(~col("name").isin(*temp_tables))

tables = table_schemas.rdd.map(lambda x: x[0]).collect()
table_ids = table_schemas.rdd.map(lambda x: x[1]).collect()
tables_sink = table_schemas.rdd.map(lambda x: x[2]).collect()

for i in range(len(tables)):
    snapshot_to_iceberg_merged(rds_id, tables[i], table_ids[i], tables_sink[i])
    # print(tables)