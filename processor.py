from asyncio.log import logger
import os, ast
from config import Config
from datetime import datetime, timezone
import logging
import boto3
from pyspark.sql import SparkSession
from path import *
from sub_processor import SubProcessor
from iceberg_catalog import IcebergCatalog
from broadcast import BroadCast
from udf import load_broadcast

import gc

# Config
AWS_PROFILE = Config.AWS_PROFILE

FILE_NAME = f"processor_execution_logs_{CURRENT_DATE}.log"
LOG_DIR = os.path.join("logs", CURRENT_DATE)
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, FILE_NAME)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

class Processor:
    def __init__(self):

        self.broadcast = BroadCast()
        self.sub_processor = SubProcessor(self.broadcast)
        self.iceberg_catalog = IcebergCatalog()


        # Prepare Session
        if DEBUG:
            self.spark_session = (
                SparkSession.builder.appName("Details Data Process")
                .master("local[*]")
                .config("spark.executor.memory", "3g")
                .config("spark.memory.fraction", "0.7")
                .config("spark.driver.memory", "4g")
                #.config("spark.executor.cores", "2")
                # .config("spark.driver.cores", "1")
                # .config("spark.executor.instances", "1")
                # .config("spark.memory.offHeap.enabled", "true")
                # .config("spark.memory.offHeap.size", "2g")
                .config(
                    "spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
                )
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", CURATED_ZONE)
                .config("spark.executor.memoryOverhead", "1g")
                .config("spark.driver.memoryOverhead", "1g")
                .config("spark.driver.cores", "1")
                .getOrCreate()
            )

        else:
            # Get AWS Profile Credentials
            self.boto3_session = boto3.Session(profile_name=AWS_PROFILE)
            self.credentials = self.boto3_session.get_credentials()
            self.aws_access_key_id = self.credentials.access_key
            self.aws_secret_access_key = self.credentials.secret_key
            self.aws_session_token = self.credentials.token

            self.spark_session = (
                SparkSession.builder.appName("Details Data Process")
                .config(
                    "spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
                    "org.apache.hadoop:hadoop-aws:2.10.1,"
                    "com.amazonaws:aws-java-sdk-bundle:1.11.1026",
                )
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", CURATED_ZONE)
                .config(
                    "spark.sql.catalog.local.hadoop.fs.s3a.access.key",
                    self.aws_access_key_id,
                )
                .config(
                    "spark.sql.catalog.local.hadoop.fs.s3a.secret.key",
                    self.aws_secret_access_key,
                )
                .config(
                    "spark.sql.catalog.local.hadoop.fs.s3a.session.token",
                    self.aws_session_token,
                )
                .config(
                    "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
                .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                )
                .config("spark.executor.memory", "7g")
                .config("spark.executor.memoryOverhead", "1g")
                .config("spark.driver.memory", "6g")
                .config("spark.driver.memoryOverhead", "1g")
                .config("spark.executor.cores", "3")
                .config("spark.driver.cores", "1")
                .config("spark.executor.instances", "8")
                .getOrCreate()
            )

    def process_details_data(
            self,
            df_details,
            df_reviews_scores,
            df_chain_and_brand,
            df_search,
    ):
        df_processed = df_details

        # Process Basic Data
        logging.info("======== Basic data process started ========")
        df_processed = self.sub_processor.process_basic_data(df_processed=df_processed, spark_session=self.spark_session)
        logging.info("======== Basic data process ended ========")

        logging.info("======== Basic data process Started ========")
        df_processed = self.sub_processor.process_review_scores_data(
            df_processed=df_processed, review_score_df=df_reviews_scores
        )
        logging.info("======== Review data process ended ========")

        logging.info("======== Property Flags data process Started ========")
        df_processed = self.sub_processor.process_property_flags_data(df_processed=df_processed)
        logging.info("======== Property Flags data process ended ========")

        logging.info("======== Chain and Brand data process Started ========")
        df_processed = self.sub_processor.process_chain_and_brand_data(
            df_processed=df_processed, df_chain_and_brand=df_chain_and_brand
        )
        logging.info("======== Chain and Brand data process Ended ========")

        # df_processed = self.sub_processor.process_property_flags_data(df_processed=df_processed)

        logging.info("======== Commission and meal plan data process Started ========")
        df_processed = self.sub_processor.process_commission_and_meal_plan_data(df_processed=df_processed, df_search=df_search)
        logging.info("======== Commission and meal plan data process Ended ========")

        logging.info("======== USD price and price history data process Started ========")
        df_processed, df_price_history = self.sub_processor.process_usd_price_and_price_history(df_processed=df_processed, df_search=df_search, spark_session=self.spark_session)
        logging.info("======== USD price and price history data process Ended ========")

        return df_processed, df_price_history

    def process_details_localize_data(self, df_processed):
        df_processed = self.sub_processor.process_localize_data(df_processed=df_processed, spark_session=self.spark_session)
        return df_processed

    def process_property_data(self):

        # # Load Details Data
        logging.info("Loading Details Data =======")
        df_details = self.spark_session.read.format("json").option("multiline", "true").load(DETAILS_DATA_DIR)
        logging.info("Details Data Loaded =======")

        # Initiate BG Location Data Processing
        logging.info("Start Location BG Process =======")
        df_processed, count_df_null_location_id = self.sub_processor.process_location_data_bg(df_details, self.spark_session)

        # Load Remaining Data
        logging.info("Loading Reviews Scores Data =======")
        df_reviews_scores = self.spark_session.read.format("json").option("multiline", "true").load(REVIEW_SCORES_DIR)
        logging.info("Reviews Scores Data Loaded =======")
        df_search = self.spark_session.read.format("json").option("multiline", "true").load(ACCOMMODATION_SEARCH_DIR)
        logging.info("Search Data Loaded =======")
        df_chain_and_brand = self.spark_session.read.format("json").option("multiline", "true").load(CHAIN_AND_BRAND)
        logging.info("Chain and Brand Data Loaded =======")
        df_reviews = self.spark_session.read.format("json").option("multiline", "true").load(REVIEW_DIR)
        logging.info("Reviews Data Loaded =======")
        # df_reviews.show(5)
        # Drop Duplicates



        df_details = df_details.dropDuplicates(["id"]).limit(100)
        df_reviews_scores = df_reviews_scores.dropDuplicates(["id"]).limit(100)
        df_search = df_search.dropDuplicates(["id"]).limit(100)
        df_reviews = df_reviews.dropDuplicates(["id"]).limit(100)
        logging.info("!!!Data count !!!")
        print(df_details.count())

        logging.info("====== Duplicates Removed =======")

        # Load Broadcast Data
        logging.info("====== Broadcasting Data =======")
        self.broadcast.prepare_broadcasted_data(spark_session=self.spark_session)
        load_broadcast(self.broadcast)
        logging.info("====== Broadcasted Data =======") 
        
        # Process Details Data
        logging.info("====== Details Data process started =======")
        df_processed, df_price_history = self.process_details_data(
            df_details=df_processed,
            df_reviews_scores=df_reviews_scores,
            df_chain_and_brand=df_chain_and_brand,
            df_search=df_search
        )


        logging.info("====== Details Data process ended =======")
        df_search = None
        df_chain_and_brand = None
        df_reviews_scores = None
        gc.collect()


        # Process Reviews Data
        logging.info("====== Reviews Data process started =======")
        df_reviews = self.sub_processor.process_reviews_data(df_processed.select('id', 'feed_provider_id', 'country_code'), df_reviews)
        logging.info("====== Reviews Data process ended =======")


        # Process Final Location Data
        logging.info("====== Location Data process started =======")
        df_processed, df_new_location_data = self.sub_processor.process_location_data(df_processed, self.spark_session, count_df_null_location_id)
        logging.info("====== Location Data process ended =======")
        
        # Process Property Name
        logging.info("====== Property Name Data process started =======")
        df_final = self.sub_processor.process_property_name_null(df_processed)
        df_processed = None
        gc.collect()
        logging.info("====== Property Name Data process ended =======")

        # Process Localize Data
        logging.info("====== Detailed localize Data process started =======")
        df_final_localize = self.process_details_localize_data(
            df_processed=df_final
        )
        logging.info("====== Detailed localize Data process ended =======")

        # Total Property Reviews Processed
        # count = df_reviews.count()
        # logging.info(f" Reviews Data Count: {count}")
        # fields_count = len(self.iceberg_catalog.tables.get("property_reviews").get("fields"))
        # logging.info(f"Reviews Fields Count: {fields_count}")

        # try:
        #     self.iceberg_catalog.upsert_data(
        #         df=df_reviews,
        #         table="property_reviews",
        #         spark_session=self.spark_session
        #     )
        #     logging.info("property_reviews upsertion Completed")
        #     df_reviews = None
        #     gc.collect()
        # except Exception as e:
        #     logging.info(f"upsertion error: {e}")


        # Total Price History Processed
        # count = df_price_history.count()
        # logging.info(f" Price History Data Count: {count}")
        # fields_count = len(self.iceberg_catalog.tables.get("price_history").get("fields"))
        # logging.info(f"Price History Fields Count: {fields_count}")

        # if df_price_history:
        #     self.iceberg_catalog.upsert_data(
        #         df=df_price_history,
        #         table="price_history",
        #         spark_session=self.spark_session,
        #         append=True,
        #     )
        #     logging.info("price_history upsertion Completed")
        #     df_price_history = None
        #     gc.collect()

        
        # Total Rental Properties Processed
        # count = df_final.count()
        # logging.info(f"Rental Property Data Count: {count}")
        # fields_count = len(self.iceberg_catalog.tables.get("rental_property").get("fields"))
        # logging.info(f"Rental Property Fields Count: {fields_count}")

        # is_updated = self.iceberg_catalog.upsert_data(
        #     df=df_final,
        #     table="rental_property",
        #     spark_session=self.spark_session
        # )
        # logging.info("rental_property upsertion Completed")
        # df_final = None
        # gc.collect()


        # Total Rental Property Localize Properties Processed
        # count = df_final_localize.count()
        # logging.info(f"Rental Property Localize Data Count: {count}")
        # fields_count = len(self.iceberg_catalog.tables.get("rental_property_localize").get("fields"))
        # logging.info(f"Rental Property Localize Fields Count: {fields_count}")

        self.iceberg_catalog.upsert_data(
            df=df_final_localize,
            table='rental_property_localize',
            spark_session=self.spark_session
        )
        logging.info("rental_property_localize upsertion Completed")
        df_final_localize = None
        gc.collect()

        # Total New Location Data (New Properties) Processed
        # count = df_new_location_data.count()
        # logging.info(f"New Location Data (New Properties) Data Count: {count}")
        # fields_count = len(self.iceberg_catalog.tables.get("rental_property_location").get("fields"))
        # logging.info(f"New Location Data (New Properties) Fields Count: {fields_count}")

        self.iceberg_catalog.upsert_data(
            df=df_new_location_data,
            table='rental_property_location',
            spark_session=self.spark_session
        )
        logging.info("rental_property_location upsertion Completed")
        self.spark_session.stop()


# Main Function
def main():

    # Start Timer
    start_time = datetime.now(timezone.utc)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Processor Start Time: {start_time_str}")

    processor = Processor()
    processor.process_property_data()

    # Stop Timer
    stop_time = datetime.now(timezone.utc)
    stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Processor Stop time: {stop_time_str}")

    execution_time = stop_time - start_time
    hours, remainder = divmod(execution_time.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = (execution_time.total_seconds() * 1000) % 1000
    formatted_execution_time = (
        f"{int(hours)} hrs, {int(minutes)} mins, {int(seconds)} secs {int(milliseconds)} millisecs"
    )
    logging.info(f"Total execution time: {formatted_execution_time}\n")


# Executer
if __name__ == "__main__":
    main()