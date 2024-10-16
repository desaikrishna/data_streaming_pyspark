import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType,StructField,StringType


def create_keyspace(session): #schema creations
    session.execute("""
            create keyspace if not exists spark_streams
                    with replication={'class': 'SimpleStrategy','replication_factor':'1'};
""")

    print("Keyspace created successfully")

def create_table(session):
    session.execute("""
    create table if not exists spark_streams.created_users(
                   id uuid primary key,
                   first_name TEXT,
                   last_name TEXT,
                   gender TEXT,
                   address TEXT,
                   post_code TEXT,
                   email TEXT,
                   username TEXT,
                   dob TEXT,
                   registered_date TEXT,
                   phone TEXT,
                   picture TEXT
    );
""")

    print("Table created successfully")

def insert_data(session, **kwargs):
    print("inserting data....")

    user_id=kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            insert into spark_stream.created_users(id, first_name,last_name,gender,address,post_code,email,username,dob,registered_date,phone,picture)
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
""",(user_id,first_name,last_name,gender,address,postcode,email,username,dob,registered_date,phone,picture))

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

def create_spark_connection():
    s_conn=None
    try:
        s_conn = SparkSession.builder \
                .appName('SparkDataStreaming') \
                .config('spark.jars.packages',
                            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
                .config('spark.cassandra.connection.host','localhost') \
                .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldnt Create the spark session due to exception {e}")

    return s_conn


#https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
#https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.13/3.5.1
def connect_to_kafka(spark_conn): #spark connection to read data from kafka
    spark_df =  None
    print('----------------------------------------------------------------------')
    print(spark_conn)
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers','localhost:9092') \
            .option('subscribe','users_created') \
            .option('startingOffsets','earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    #connecting to the cassandra cluster
    try:
        cluster = Cluster(['localhost'],port=9042)
        cas_session = cluster.connect()
        logging.info("-------------Cassandra Connection Successfully---------------")
        print(cas_session)

        return cas_session

    except Exception as e:
        logging.error(f"Couldnt create cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):

    print('-----------------------------')
    print(spark_df)
    schema = StructType([ 
        StructField("id",StringType(),False),
        StructField("first_name",StringType(),False),
        StructField("last_name",StringType(),False),
        StructField("gender",StringType(),False),
        StructField("address",StringType(),False),
        StructField("post_code",StringType(),False),
        StructField("email",StringType(),False),
        StructField("username",StringType(),False),
        StructField("dob",StringType(),False),
        StructField("registered_date",StringType(),False),
        StructField("phone",StringType(),False),
        StructField("picture",StringType(),False)

    ])


    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    # sel=spark_df.selectExpr("CAST(value as STRING) as value") \
    #     .select(from_json(col('value'),schema).alias('data')).select("data*")

    # sel = spark_df.selectExpr("CAST(value AS STRING) as value") \
    #     .select(from_json(col('value'), schema).alias('data')).select("data.*")

    # json_df = spark_df.selectExpr("cast(value as string) as value")
    # json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], schema)).select("value.*") 
    
    # print('------------------------------json_expanded_df----------------------------------------')   
    # print(json_expanded_df)
    # return json_expanded_df

    print('------------------------------sel----------------------------------------')   

    #print(sel)
    #sel.show()
    return sel

if __name__ == "__main__":
    #create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        #connect to kafka with spark connection
        spark_df=connect_to_kafka(spark_conn)
        # print('----------------------------------------------------------------------')
        # print(spark_conn)
        selection_df=create_selection_df_from_kafka(spark_df) #creating a schema to be input into cassandra(not sure)
        print('------------------------------selection_df----------------------------------------')
        #print(selection_df)
        #selection_df.show()
        session = create_cassandra_connection()


        if session is not None:
            print("----------------session------------------------")
            print(session)
            create_keyspace(session)
            create_table(session)
            #insert_data(session)
            streaming_query=(selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
                        .option('checkpointLocation','/tmp/checkpoint') \
                        .option('keyspace','spark_streams') \
                        .option('table','created_users') \
                        .start())

        # query = selection_df.selectExpr("CAST(id AS STRING)", "CAST(first_name AS STRING)") \
        # .writeStream \
        # .outputMode("append") \
        # .format("console") \
        # .start()

        # query.awaitTermination()
            
            streaming_query.awaitTermination()

            