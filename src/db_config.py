from pyspark.sql import SparkSession

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "etl_db"
DB_USER = "postgres"
DB_PASSWORD = "Reema@123" #give your own db password
JDBC_DRIVER_PATH = "/Users/vyomkeshharshwardhan/Downloads/postgresql-42.7.5.jar"

def get_spark_session(app_name = "Postgres_Pyspark_Connection"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", JDBC_DRIVER_PATH) \
        .getOrCreate()
    
    return spark

def get_jdbc_url():
    return f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

def read_table(spark, table_name):
    jdbc_url = get_jdbc_url()
    return spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def write_table(df, table_name, mode="append"):
    jdbc_url = get_jdbc_url()
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()