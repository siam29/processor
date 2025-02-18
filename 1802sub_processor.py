import logging
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row
from pyspark.sql.window import Window
from datetime import datetime

import udf as pudf
from itertools import chain
from path import *
from util import get_keyword_lists
from pyspark.sql.functions import col, lit, create_map, when
import os
import asyncio
import requests
import time
import logging


FILE_NAME = f"processor_execution_logs_{CURRENT_DATE}.log"
LOG_DIR = os.path.join("logs", CURRENT_DATE)
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, FILE_NAME)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

class SubProcessor:
    
    def __init__(self, broadcast):
        self.broadcast = broadcast

    def process_basic_data(self, df_processed, spark_session):
        df_processed = self.process_property_name(df_processed)
        logging.info(">> sub-process : property_name() completed =====")
        df_processed = self.process_rating_and_score(df_processed)
        logging.info(">> sub-process : process_rating_and_score() completed =====")
        df_processed = self.process_checkin_checkout_times(df_processed)
        logging.info(">> sub-process : process_checkin_checkout_times() completed =====")
        df_processed = self.process_property_slug(df_processed)
        logging.info(">> sub-process : process_property_slug() completed =====")
        df_processed = self.process_property_type(df_processed)
        logging.info(">> sub-process : process_property_type() completed =====")
        df_processed = self.process_property_type_category(df_processed)
        logging.info(">> sub-process : process_property_type_category() completed =====")
        df_processed = self.process_property_type_categories(df_processed)
        logging.info(">> sub-process : process_property_type_categories() completed =====")
        df_processed = self.process_room_type(df_processed)
        logging.info(">> sub-process : process_room_type() completed =====")
        df_processed = self.process_amenities_data(df_processed, spark_session)
        logging.info(">> sub-process : process_amenities_data() completed =====")
        df_processed = self.process_policies_data(df_processed)
        logging.info(">> sub-process : process_policies_data() completed =====")
        df_processed = self.process_min_stay(df_processed)
        logging.info(">> sub-process : process_min_stay() completed =====")
        df_processed = self.process_hotel_photos(df_processed)
        logging.info(">> sub-process : process_hotel_photos() completed =====")
        df_processed = self.process_rooms_data(df_processed)
        logging.info(">> sub-process : process_rooms_data() completed =====")
        df_processed = self.process_feed_provider_url_and_licence(df_processed)
        logging.info(">> sub-process : process_feed_provider_url_and_licence() completed =====")
        df_processed = self.prepare_is_published(df_processed)
        logging.info(">> sub-process : prepare_is_published() completed =====")
        #df_processed.show(5)

        return df_processed
    
    def process_feed_provider_url_and_licence(self, df_processed):
        df_processed = df_processed.withColumn(
            "feed_provider_url",
            F.when(
                F.col("url").isNotNull(),
                F.concat(
                    F.split(F.col("url"), "=")[0],
                    F.lit("=affiliate_id")
                )
            ).otherwise(F.lit(None))
        )
        df_processed = df_processed.withColumn(
            "license_number",
            F.when(
                F.col("description.license_numbers").isNotNull(),
                F.concat_ws(", ", F.col("description.license_numbers")),
            ).otherwise(None),
        )
        # TODO: for now we are adding languages field with initial value 'en-us', need to update.
        # df_processed = df_processed.withColumn("languages", F.array(F.lit("en-us")))
        df_processed = df_processed.withColumn(
            "languages",
            F.array(
                F.lit("en-us"), F.lit("fr"), F.lit("es"), F.lit("de")
            ),
        )
        return df_processed

    def process_intials(self, df_processed):

        df_processed = df_processed \
        .withColumn("feed", F.lit(11)) \
        .withColumn("feed_provider_id", F.col("id")) \
        .withColumn("currency", F.when(F.col("currency").isNotNull(), F.col("currency")).otherwise("USD")) \
        .withColumn("id", F.concat(F.lit("BC-"), F.col("id").cast("string"))) \
        .withColumn("owner_id", F.lit(None))

        return df_processed

    def process_location_data_bg(self, df_processed, spark_session):

        df_processed = self.process_country_code(df_processed)
        df_processed = self.process_intials(
            df_processed=df_processed,
        )

        # Obtain lat, lon
        df_processed = df_processed \
        .withColumn("lat", F.coalesce(F.col("location.coordinates.latitude"), F.lit(0))) \
        .withColumn("lon", F.coalesce(F.col("location.coordinates.longitude"), F.lit(0))) \

        # Existing Data
        if spark_session.catalog.tableExists("local.rental_property_location"):
            df_rp_existing = spark_session.read.format("iceberg").load("local.rental_property_location")
            df_processed = df_processed.join(
                df_rp_existing,
                on="id",
                how="left"
            ) \
            .dropDuplicates(["id"]) \

        else:
            df_processed = df_processed \
            .withColumn("location_id", F.lit(None)) \
            .withColumn("parent_path", F.lit(None)) \
            .withColumn("categories", F.lit(None)) \
            .withColumn("state", F.lit(None)) \
            .withColumn("state_abbr", F.lit(None)) \

        

        df_null_location_id = df_processed \
            .filter(df_processed["location_id"].isNull()) \
            .select("id", "lat", "lon", "country_code") \
            .dropDuplicates(["id"])
        
        count_df_null_location_id = df_null_location_id.count()
        df_null_location_id = df_null_location_id.toPandas()
        
        # Write Location Data
        if Config.DEBUG and (count_df_null_location_id > 0):
            os.makedirs(LOCATION_DATA_INPUT, exist_ok=True)
            df_null_location_id.to_csv(f"{LOCATION_DATA_INPUT}/rental_property_location.csv", index=False)
        else:
            if count_df_null_location_id > 0:
                asyncio.run(self.broadcast.utils.s3_df_to_csv_upload(
                    df=df_null_location_id, 
                    path=f"{LOCATION_DATA_INPUT}/rental_property_location.csv"
                ))

                # Make Location API Call
                try:

                    data={
                        "s3_filepath": f"{LOCATION_DATA_INPUT}/rental_property_location.csv",
                        "date": CURRENT_DATE
                    }
                    url = f"{Config.LOCATION_SERVICE_URL}/api/v1/initiate-location-assign/"

                    response = requests.post(
                        url=url, 
                        json=data
                    )
                    response.raise_for_status()
                    if response.status_code == 200:
                        logging.info(f"BG Location Data Processing Initiated")

                except requests.exceptions.RequestException as e:
                    logging.error(f"BG Location Data Processing Failed to Initiate!: {str(e)}")

        return df_processed, count_df_null_location_id

    
    def process_property_name(self, df_processed):
        
        df_processed = df_processed.withColumn("property_name", F.col("name.en-us"))
        return df_processed


    def process_property_name_null(self, df_processed):
        
        df_processed = df_processed \
        .withColumn(
            "property_name",
            F.when(
                F.col("property_name").isNull(),
                F.when(
                    F.col("property_type").isNotNull() & F.col("display").isNotNull(),
                    F.concat(F.col("property_type"), F.lit(" in "), F.col("display"))
                ).otherwise(F.col("property_name"))
            ).otherwise(F.col("property_name"))
        )

        return df_processed
  

    def process_checkin_checkout_times(self, df_processed):

        df_processed = df_processed \
        .withColumn("checkin_from", F.col("checkin_checkout_times.checkin_from")) \
        .withColumn("checkin_to", F.col("checkin_checkout_times.checkin_to")) \
        .withColumn("checkout_from", F.col("checkin_checkout_times.checkout_from")) \
        .withColumn("checkout_to", F.col("checkin_checkout_times.checkout_to")) \

        return df_processed

    def process_rating_and_score(self, df_processed):

        df_processed = df_processed \
        .withColumn(
            "number_of_review",
            F.when(F.col("rating.number_of_reviews").isNotNull(), F.col("rating.number_of_reviews"))
            .otherwise(0)
            .cast("int"),
        ) \
        .withColumn(
            "star_rating",
            F.when(
                F.col("rating.stars").isNotNull(), F.floor(F.col("rating.stars"))
            ).otherwise(0)
        ) \
        .withColumn(
            "review_score",
            F.format_number(
                F.when(
                    F.col("rating.review_score").isNotNull(), F.col("rating.review_score")
                ).otherwise(0),
                2,
            ).cast("float")
        ) \
        .withColumn(
            "review_score_general",
            F.when(
                F.col("review_score").isNotNull() & (F.col("review_score") != 0),
                F.col("review_score") / 2,
            )
            .otherwise(0)
            .cast(T.DecimalType(10, 2))
        ) \
        .withColumn(
            "is_preferred",
            F.when(F.col("rating.preferred").isNotNull(), F.col("rating.preferred")).otherwise(
                False
            )
        ) 

        return df_processed
    
    def process_country_code(self, df_processed):
        
        df_processed = df_processed.withColumn(
            "country_code",
            F.when(F.col("location.country").isNotNull(), F.upper(F.trim(F.col("location.country"))))
            .when(F.upper(F.trim(F.col("location.country"))) == "XA", "GE")
            .when(F.upper(F.trim(F.col("location.country"))) == "XY", "CY")
            .otherwise(None)
        )


        return df_processed


    def process_property_slug(self, df_processed):

        df_processed = df_processed \
        .withColumn(
            "property_slug",
            F.lower(F.regexp_replace(F.trim(F.col("property_name")), r"[^a-zA-Z0-9]+", "-"))
        ) \
        .withColumn("updated_at", F.lit(datetime.now().date()))

        return df_processed
    
    def process_room_type(self, df_processed):
        
        shared_room_keywords = ["shared room", "room shared", "bathroom", "dormitory", "hostel"]
        shared_space_keywords = ["apartment", "flat", "house", "home", "condo"]

        df_processed = df_processed \
        .withColumn(
            "room_type",
            F.when(
                F.lower(F.col("property_name")).rlike("|".join(shared_room_keywords)), 
                "Shared room"
            ).when(
                F.lower(F.col("property_name")).rlike("|".join(shared_space_keywords)), 
                "Shared space"
            ).otherwise(None)
        )

        return df_processed
    
    def process_property_type(self, df_processed):
        
        df_processed = df_processed.withColumn(
            "property_type", 
            pudf.prepare_property_type(df_processed["accommodation_type"])
        )

        return df_processed
    
    def process_property_type_category(self, df_processed):

        df_processed = df_processed.withColumn(
            "property_type_category",
            pudf.prepare_property_type_category(df_processed["property_type"])
        )

        return df_processed
    
    def process_property_type_categories(self, df_processed):
        df_processed = df_processed.withColumn(
            "property_type_categories",
            pudf.prepare_property_type_categories(df_processed["property_name"], df_processed["property_type"], df_processed["property_type_category"])
        )

        return df_processed

    def prepare_is_published(self, df_processed):
        df_processed = df_processed.withColumn(
            "published",
             F.when(
                (F.col("feed_provider_url").isNotNull()) & (F.col("feed_provider_url") != ""),
                True
            ).otherwise(False)
        )
        return df_processed

    def process_amenities_data(self, df_processed, spark_session):
        accommodation_themes = self.broadcast.accommodation_themes
        themes_data = {
            int(key): value["name"]["en-us"]
            for key, value in accommodation_themes.items()
        }
        map_pairs = list(chain.from_iterable([(k, v) for k, v in themes_data.items()]))
        themes_map = F.create_map([F.lit(item) for item in map_pairs])
        df_processed = df_processed.withColumn("themes_id_map", themes_map)
        df_processed = df_processed.withColumn(
            "themes",
            F.expr(
                """
                    transform(
                        themes,
                        x -> themes_id_map[cast(x as string)]
                    )
                """
            ),
        ).withColumn("themes", F.expr("filter(themes, x -> x is not null)"))

        df_processed = df_processed.withColumn(
            "amenity_ids", F.expr("transform(facilities, x -> x.id)")
        )
        accommodation_facilities = self.broadcast.accommodation_facilities
        facilities_data = {
            int(key): value["name"]["en-us"]
            for key, value in accommodation_facilities.items()
        }
        map_pairs = list(
            chain.from_iterable([(k, v) for k, v in facilities_data.items()])
        )
        amenity_map = F.create_map([F.lit(item) for item in map_pairs])
        df_processed = df_processed.withColumn("amenity_id_map", amenity_map)
        df_processed = df_processed.withColumn(
            "amenities",
            F.expr(
                """
                    transform(
                        amenity_ids,
                        x -> amenity_id_map[cast(x as string)]
                    )
                """
            ),
        ).withColumn("amenities", F.expr("filter(amenities, x -> x is not null)"))

        amenity_category_synonyms = self.broadcast.amenity_category_synonyms
        rows = [
            Row(slugified_amenity=key, category=value if isinstance(value, list) else [value])
            for key, value in amenity_category_synonyms.items()
        ]

        # Create a DataFrame from the list of Row objects
        amenity_synonyms_df = spark_session.createDataFrame(rows)
        new_df_processed = (
            df_processed.withColumn(
                "slugified_amenities",
                F.expr("transform(amenities, x -> lower(regexp_replace(x, '[^a-zA-Z0-9]', '-')))"),
            )
            .withColumn("exploded_amenities", F.explode_outer(F.col("slugified_amenities")))
            .join(
                amenity_synonyms_df,
                F.col("exploded_amenities") == F.col("slugified_amenity"),
                "left",
            )
            .groupBy("id")
            .agg(F.collect_list("category").alias("new_amenity_categories"))
        )
        new_df_processed = new_df_processed.withColumn("flat_new_amenity_categories", F.flatten(F.col("new_amenity_categories")))
        df_processed = df_processed.join(new_df_processed, on="id", how="left") \
                    .withColumn("amenity_categories", F.col("flat_new_amenity_categories"))
        
        df_processed = df_processed.withColumn(
            "amenity_categories",
            F.array_distinct(F.col("amenity_categories"))
        )
        
        return df_processed

    def process_policies_data(self, df_processed):
        df_processed = df_processed.withColumn(
            "minimum_guest_age", F.col("policies.minimum_guest_age")
        )
        df_processed = df_processed.withColumn(
            "amenity_categories",
            F.when(
                F.col("minimum_guest_age") == 0,
                F.array_union(
                    F.col("amenity_categories"), F.array(F.lit("Child Friendly"))
                ),
            ).otherwise(F.col("amenity_categories")),
        )
        df_processed = df_processed.withColumn(
            "adult_only",
            F.when(F.col("minimum_guest_age") >= 18, F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
        df_processed = df_processed.withColumn(
            "is_pet_friendly",
            F.when(F.col("policies.pets.allowed") == "yes", F.lit(True)).otherwise(
                F.lit(False)
            ),
        ).withColumn(
            "pet_policy",
            F.when(F.col("policies.pets.allowed") == "yes", F.lit("Pets are allowed."))
            .when(
                F.col("policies.pets.allowed") == "upon_request",
                F.concat(
                    F.lit("Pets are allowed upon request"),
                    F.when(
                        F.col("policies.pets.charge_mode").isNotNull(),
                        F.concat(
                            F.lit(" and "),
                            F.regexp_replace(
                                F.col("policies.pets.charge_mode"), "_", " "
                            ),
                        ),
                    ).otherwise(F.lit(".")),
                ),
            )
            .when(
                F.col("policies.pets.allowed") == "no", F.lit("Pets are not allowed.")
            )
            .otherwise(F.lit(None)),
        )
        df_processed = df_processed.withColumn(
            "infant_policy",
            F.when(
                (F.size(F.col("policies.cots_and_extra_beds")) > 0)
                & (F.col("policies.cots_and_extra_beds")[0]["age"]["to"].isNotNull())
                & (F.col("policies.cots_and_extra_beds")[0]["age"]["to"] <= 2)
                & (
                    F.col("policies.cots_and_extra_beds")[0]["age"]["from"]
                    < F.col("policies.cots_and_extra_beds")[0]["age"]["to"]
                ),
                F.concat(
                    F.lit("Cots are available from age "),
                    F.col("policies.cots_and_extra_beds")[0]["age"]["from"].cast(
                        "string"
                    ),
                    F.lit(" to "),
                    F.col("policies.cots_and_extra_beds")[0]["age"]["to"].cast(
                        "string"
                    ),
                    F.when(
                        F.col("policies.cots_and_extra_beds")[0]["price"].isNotNull()
                        & (F.col("policies.cots_and_extra_beds")[0]["price"] > 0),
                        F.concat(
                            F.lit(" with price "),
                            F.col("policies.cots_and_extra_beds")[0]["price"].cast(
                                "string"
                            ),
                        ),
                    ).otherwise(F.lit(" without price")),
                    F.lit(" in "),
                    F.col("policies.cots_and_extra_beds")[0]["type"],
                ),
            ).otherwise(None),
        )
        df_processed = df_processed.withColumn(
            "child_policy",
            F.when(
                (F.col("policies.cots_and_extra_beds")[0]["age"]["to"] > 2)
                & (
                    F.col("policies.cots_and_extra_beds")[0]["age"]["from"]
                    < F.col("policies.cots_and_extra_beds")[0]["age"]["to"]
                ),
                F.concat(
                    F.lit("Children aged "),
                    F.col("policies.cots_and_extra_beds")[0]["age"]["to"].cast(
                        "string"
                    ),
                    F.lit(" years and above are considered adults at this property."),
                ),
            ),
        )
        df_processed = df_processed.withColumn(
            "checkin_policy",
            F.concat_ws(
                " and ",
                F.when(
                    F.col("policies.minimum_checkin_age").isNotNull(),
                    F.concat(
                        F.lit("The minimum age for check-in is "),
                        F.col("policies.minimum_checkin_age").cast("string"),
                    ),
                ),
                F.when(
                    F.col("policies.maximum_checkin_age").isNotNull(),
                    F.concat(
                        F.lit("The maximum age for check-in is "),
                        F.col("policies.maximum_checkin_age").cast("string"),
                    ),
                ),
            ),
        )
        df_processed = df_processed.withColumn(
            "adult_policy",
            F.when(
                (F.col("minimum_guest_age").isNotNull())
                & (F.col("minimum_guest_age") >= 18),
                F.concat(
                    F.lit("Minimum guest age is "),
                    F.col("minimum_guest_age").cast("string"),
                ),
            ),
        )

        df_processed = df_processed.withColumn(
            "payment_policy",
            F.when(
                (F.col("payment.methods.cash").isNotNull())

                & (~F.col("payment.methods.cash")),
                F.lit("Credit Card Required"),
            ),
        )

        df_processed = df_processed.withColumn(
            "other_policy",
            F.trim(F.expr("description['important_information']['en-us']")),
        )

        df_processed = df_processed.withColumn(
            "policy",
            F.struct(
                F.col("other_policy").alias("other_policy"),
                F.lit(None).cast(T.StringType()).alias("cancellation_policy"),
                F.col("is_pet_friendly").alias("is_pet_friendly"),
                F.col("adult_only").alias("adult_only"),
                F.col("infant_policy").alias("infant_policy"),
                F.col("child_policy").alias("child_policy"),
                F.col("checkin_policy").alias("checkin_policy"),
                F.col("pet_policy").alias("pet_policy"),
                F.col("adult_policy").alias("adult_policy"),
                F.col("payment_policy").alias("payment_policy"),
            )
        )
        columns_to_drop = [
            "minimum_guest_age",
            "other_policy",
            "is_pet_friendly",
            "adult_only",
            "infant_policy",
            "child_policy",
            "checkin_policy",
            "pet_policy",
            "adult_policy",
            "payment_policy",
        ]
        df_processed = df_processed.drop(*columns_to_drop)
        return df_processed

    def process_location_data(self, df_processed, spark_session, count_df_null_location_id):

        # Load Required Data
        
        # - City
        df_city = spark_session.read.format("json").option("multiline", "true").load(LOCATION_MAPPING_CITY_DIR)

        # - Code Country
        df_code_country = spark_session.read.format("json").option("multiline", "true").load(CODE_COUNTRY) \
        .withColumnRenamed("Code", "country_code") \
        .withColumnRenamed("Name", "country_name")
        
        # Process Data

        # - Latlon and Zip Code
        df_processed = df_processed \
        .withColumn(
            "latlon",
                F.when(F.col("location.coordinates.latitude").isNotNull() & F.col("location.coordinates.longitude").isNotNull(), \
                    F.concat_ws(",", F.col("location.coordinates.latitude"), F.col("location.coordinates.longitude"))) \
                .otherwise(None)
        ) \
        .withColumn("zip_code",
            F.when(
                (F.col("location.postal_code") == "") | 
                (F.col("location.postal_code").isNull()), 
                None
            ).otherwise(F.col("location.postal_code"))
        )

        # - Process Country
        df_processed = df_processed.join(
            df_code_country,
            on="country_code",
            how="left"
        ) \
        .withColumn(
            "country",
             F.when(F.col("country_name") == "United States", "USA").otherwise(F.col("country_name"))
        ) \
        .drop("country_name") \
        .dropDuplicates(["id"])


        # - Process City
        df_processed = df_processed \
        .withColumn(
            "city_code",
            F.col("location.city")
        )

        df_processed = df_processed.join(
            df_city,
            on="city_code",
            how="left"
        ) \
        .dropDuplicates(["id"]) \
        .drop("city_code")
        
        # - Process Location ID, Parent Path, Categories, State, State Abbr
        location_data_schema = F.StructType([
            T.StructField("id", T.IntegerType(), True),
            T.StructField("location_id", T.StringType(), True),
            T.StructField("parent_path", T.ArrayType(T.StringType(), True), True),
            T.StructField("categories", T.StringType(), True),
            T.StructField("state", T.StringType(), True),
            T.StructField("state_abbr", T.StringType(), True)
        ])
        df_location_data = spark_session.createDataFrame([], schema=location_data_schema)

        try:
            if count_df_null_location_id > 0:
                received = False
                while not received:
                    if Config.DEBUG:
                        if os.path.exists(LOCATION_DATA_OUTPUT) and os.path.isdir(LOCATION_DATA_OUTPUT):
                            received = True
                    else:
                        received = asyncio.run(self.broadcast.utils.check_s3_file_exists(f"{LOCATION_DATA_OUTPUT}/final.json"))
                        if not received:
                            logging.info("Waiting for location Data...")
                            time.sleep(5)

                logging.info("Location Data Received!")
                
                if Config.DEBUG:
                    df_location_data = spark_session.read.format("json").option("multiline", "true").load(f"{LOCATION_DATA_OUTPUT}")
                else:
                    df_location_data = spark_session.read.format("json").option("multiline", "true").load(f"s3a://{S3_BUCKET}/{LOCATION_DATA_OUTPUT}")

                df_location_data = df_location_data.select(
                    F.col("id"),
                    F.col("location_id").alias("dl_location_id"),
                    F.col("parent_path").alias("dl_parent_path"),
                    F.col("categories").alias("dl_categories"),
                    F.col("state").alias("dl_state"),
                    F.col("state_abbr").alias("dl_state_abbr"),
                )
                    
                df_processed = df_processed.join(
                    df_location_data, on="id", how="left"
                )

                df_processed = df_processed \
                .withColumn("location_id", F.coalesce(F.col("location_id"), F.col("dl_location_id"))) \
                .withColumn("parent_path", F.coalesce(F.col("parent_path"), F.col("dl_parent_path"))) \
                .withColumn("categories", F.coalesce(F.col("categories"), F.col("dl_categories"))) \
                .withColumn("state", F.coalesce(F.col("state"), F.col("dl_state"))) \
                .withColumn("state_abbr", F.coalesce(F.col("state_abbr"), F.col("dl_state_abbr")))

                df_processed = df_processed.drop("dl_location_id", "dl_parent_path", "dl_categories", "dl_state", "dl_state_abbr")

            else:
                df_processed = df_processed \
                .withColumn("location_id", F.when(F.col("location_id").isNotNull(), F.col("location_id")).otherwise(F.lit(None))) \
                .withColumn("parent_path", F.when(F.col("parent_path").isNotNull(), F.col("parent_path")).otherwise(F.lit(None))) \
                .withColumn("categories", F.when(F.col("categories").isNotNull(), F.col("categories")).otherwise(F.lit(None))) \
                .withColumn("state", F.when(F.col("state").isNotNull(), F.col("state")).otherwise(F.lit(None))) \
                .withColumn("state_abbr", F.when(F.col("state_abbr").isNotNull(), F.col("state_abbr")).otherwise(F.lit(None)))

        except Exception as e:
            logging.info(f"Location Data Not Received! {str(e)}")

            df_processed = df_processed \
            .withColumn("location_id", F.when(F.col("location_id").isNotNull(), F.col("location_id")).otherwise(F.lit(None))) \
            .withColumn("parent_path", F.when(F.col("parent_path").isNotNull(), F.col("parent_path")).otherwise(F.lit(None))) \
            .withColumn("categories", F.when(F.col("categories").isNotNull(), F.col("categories")).otherwise(F.lit(None))) \
            .withColumn("state", F.when(F.col("state").isNotNull(), F.col("state")).otherwise(F.lit(None))) \
            .withColumn("state_abbr", F.when(F.col("state_abbr").isNotNull(), F.col("state_abbr")).otherwise(F.lit(None)))
            
        # - Process Display
        df_processed = df_processed.withColumn(
            "display",
            F.concat_ws(
                ", ",
                F.when(F.col("city").isNotNull() & (F.col("city") != ""), F.col("city")).otherwise(None),
                F.when(F.col("state").isNotNull() & (F.col("state") != ""), F.col("state"))
                .otherwise(F.when(F.col("state_abbr").isNotNull() & (F.col("state_abbr") != ""), F.col("state_abbr")).otherwise(None)),
                F.when(F.col("country").isNotNull() & (F.col("country") != ""), F.col("country"))
                .otherwise(F.when(F.col("country_code").isNotNull() & (F.col("country_code") != ""), F.col("country_code")).otherwise(None))
            )
        )

        # - New Properties with Assigned Location IDs
        df_location_data = df_location_data.select(
                F.col("id"),
                F.col("dl_location_id").alias("location_id"),
                F.col("dl_parent_path").alias("parent_path"),
                F.col("dl_categories").alias("categories"),
                F.col("dl_state").alias("state"),
                F.col("dl_state_abbr").alias("state_abbr"),
            )

        return df_processed, df_location_data

    def process_review_scores_data(self, df_processed, review_score_df):
        """
        Pipeline to process review scores data.
        """
        try:
            review_score_df = (
                review_score_df.withColumn(
                    "cleanliness", F.col("breakdown.cleanliness.score")
                )
                .withColumn("comfort", F.col("breakdown.comfort.score"))
                .withColumn("facilities", F.col("breakdown.facilities.score"))
                .withColumn("free_wifi", F.col("breakdown.free_wifi.score"))
                .withColumn("location", F.col("breakdown.location.score"))
                .withColumn("staff", F.col("breakdown.staff.score"))
                .withColumn("value_for_money", F.col("breakdown.value_for_money.score"))
            )

            review_score_df = (
                review_score_df.withColumn(
                    "review_scores",
                    F.struct(
                        F.col("cleanliness").cast("float").alias("cleanliness"),
                        F.col("comfort").cast("float").alias("comfort"),
                        F.col("facilities").cast("float").alias("facilities"),
                        F.col("free_wifi").cast("float").alias("free_wifi"),
                        F.col("location").cast("float").alias("location"),
                        F.col("staff").cast("float").alias("staff"),
                        F.col("value_for_money").cast("float").alias("value_for_money"),
                    ),
                )
                .withColumn("review_score", F.col("score").cast("float"))
                .withColumn(
                    "review_score_general",
                    F.round(F.col("score") / 2, 1).cast("decimal(12, 2)"),
                )
                .withColumn("id", F.concat(F.lit("BC-"), F.col("id").cast("string")))
            )

            final_review_score_df = review_score_df.select(
                F.col("id").alias("id"),
                F.col("review_scores"),
                F.col("review_score").alias("review_score_new"),
                F.col("review_score_general").alias("review_score_general_new"),
                F.col("number_of_reviews").cast("int").alias("number_of_review_new"),
            )
            df_processed = df_processed.join(final_review_score_df, on="id", how="left")

            update_columns = {
                "review_score": "review_score_new",
                "review_score_general": "review_score_general_new",
                "number_of_review": "number_of_review_new",
            }

            for target_col, source_col in update_columns.items():
                df_processed = df_processed.withColumn(
                    target_col,
                    F.coalesce(
                        F.when(
                            (F.col(source_col).isNotNull()) & (F.col(source_col) != 0),
                            F.col(source_col),
                        ),
                        F.col(target_col),
                    ),
                )
            df_processed = df_processed.drop(
                "review_score_new", "review_score_general_new", "number_of_review_new"
            )

        except Exception as error:
            logging.error(f"Error in process_review_scores_data: {error}")

        return df_processed




    def after_process_review_scores_data(self, df_processed, review_score_df):
        """
        Pipeline to process review scores data.
        """
        try:
            # Consolidating the 'withColumn' transformations for better performance
            review_score_df = (
                review_score_df
                .withColumn(
                    "review_scores",
                    F.struct(
                        F.col("breakdown.cleanliness.score").cast("float").alias("cleanliness"),
                        F.col("breakdown.comfort.score").cast("float").alias("comfort"),
                        F.col("breakdown.facilities.score").cast("float").alias("facilities"),
                        F.col("breakdown.free_wifi.score").cast("float").alias("free_wifi"),
                        F.col("breakdown.location.score").cast("float").alias("location"),
                        F.col("breakdown.staff.score").cast("float").alias("staff"),
                        F.col("breakdown.value_for_money.score").cast("float").alias("value_for_money")
                    )
                )
                .withColumn(
                    "review_score", F.col("score").cast("float")
                )
                .withColumn(
                    "review_score_general", F.round(F.col("score") / 2, 1).cast("decimal(12, 2)")
                )
                .withColumn(
                    "id", F.concat(F.lit("BC-"), F.col("id").cast("string"))
                )
            )

            final_review_score_df = review_score_df.select(
                F.col("id").alias("id"),
                F.col("review_scores"),
                F.col("review_score").alias("review_score_new"),
                F.col("review_score_general").alias("review_score_general_new"),
                F.col("number_of_reviews").cast("int").alias("number_of_review_new")
            )

            # Perform the join efficiently
            df_processed = df_processed.join(final_review_score_df, on="id", how="left")

            # Combine multiple 'withColumn' operations for updating columns
            update_columns = {
                "review_score": "review_score_new",
                "review_score_general": "review_score_general_new",
                "number_of_review": "number_of_review_new"
            }

            for target_col, source_col in update_columns.items():
                df_processed = df_processed.withColumn(
                    target_col,
                    F.coalesce(F.when(F.col(source_col).isNotNull() & (F.col(source_col) != 0), F.col(source_col)), F.col(target_col))
                )

            # Drop unnecessary columns
            df_processed = df_processed.drop("review_score_new", "review_score_general_new", "number_of_review_new")

        except Exception as error:
            logging.error(f"Error in process_review_scores_data: {error}")
        logging.info("==== : After process reviews data : ======")

        return df_processed





    def process_chain_and_brand_data(self, df_processed, df_chain_and_brand):
        try:
            df_chain_and_brand = df_chain_and_brand.withColumn(
                "brands", F.explode("brands")
            ).select(
                F.col("id").cast("long").alias("chain_id"),
                F.col("name").cast("string").alias("chain_name"),
                F.col("brands")["id"].cast("long").alias("brand_id"),
                F.col("brands")["name"].cast("string").alias("brand_name"),
            )

            df_processed = df_processed.join(
                df_chain_and_brand,
                df_processed["brands"].getItem(0) == df_chain_and_brand["brand_id"],
                "left",
            )

            df_processed = df_processed.withColumn(
                "chain_and_brand",
                F.struct(
                    F.col("chain_id"),
                    F.col("chain_name"),
                    F.col("brand_id"),
                    F.col("brand_name"),
                ),
            )

            df_processed = df_processed.drop("chain_id", "chain_name", "brand_name")

        except Exception as error:
            logging.error(f"Error in process_chain_and_brand_data: {error}")

        return df_processed
    
    def after_process_chain_and_brand_data(self, df_processed, df_chain_and_brand):
        try:
            # Explode "brands" in df_chain_and_brand efficiently
            df_chain_and_brand = df_chain_and_brand.withColumn("brands", F.explode("brands")).select(
                F.col("id").cast("long").alias("chain_id"),
                F.col("name").cast("string").alias("chain_name"),
                F.col("brands.id").cast("long").alias("brand_id"),
                F.col("brands.name").cast("string").alias("brand_name")
            )

            # Join using brands[0] directly without an extra explode
            df_processed = df_processed.join(
                df_chain_and_brand,
                df_processed["brands"].getItem(0) == df_chain_and_brand["brand_id"],
                "left"
            )

            # Struct column creation
            df_processed = df_processed.withColumn(
                "chain_and_brand",
                F.struct("chain_id", "chain_name", "brand_id", "brand_name")
            ).drop("chain_id", "chain_name", "brand_name")  # Drop unnecessary columns

        except Exception as error:
            logging.error(f"Error in process_chain_and_brand_data: {error}")
        logging.info("=== : After Process Chain and Brand Data :====")

        return df_processed

    # def after_process_chain_and_brand_data(self, df_processed, df_chain_and_brand):
    #     try:
    #         # Flatten the "brands" array in df_chain_and_brand
    #         df_chain_and_brand = df_chain_and_brand.select(
    #             F.col("id").cast("long").alias("chain_id"),
    #             F.col("name").cast("string").alias("chain_name"),
    #             F.explode("brands").alias("brand"),
    #         ).select(
    #             "chain_id", "chain_name", 
    #             F.col("brand.id").cast("long").alias("brand_id"),
    #             F.col("brand.name").cast("string").alias("brand_name")
    #         )
            
    #         # Explode the "brands" array in df_processed before joining
    #         df_processed = df_processed.withColumn("brand_exploded", F.explode_outer("brands"))

    #         # Join on exploded brand ID
    #         df_processed = df_processed.join(
    #             df_chain_and_brand,
    #             df_processed["brand_exploded"] == df_chain_and_brand["brand_id"],
    #             "left"
    #         )

    #         # Create the "chain_and_brand" struct column
    #         df_processed = df_processed.withColumn(
    #             "chain_and_brand", 
    #             F.struct("chain_id", "chain_name", "brand_id", "brand_name")
    #         )

    #         # Drop unnecessary columns
    #         df_processed = df_processed.drop("chain_id", "chain_name", "brand_exploded", "brand_name")

    #     except Exception as error:
    #         logging.error(f"Error in process_chain_and_brand_data: {error}")
    #     logging.info("=== : After Process Chain and Brand Data :====")

    #     return df_processed

    
    def process_min_stay(self, df_processed):
        try:
            df_processed = df_processed.withColumn("min_stay", F.lit(1))
        except Exception as error:
            logging.error(f"Error in process_min_stay: {error}")
        return df_processed

    def process_hotel_photos(self, df_processed):
        try:
            df_exploded = df_processed.withColumn(
                "photo", F.explode_outer(F.col("photos"))
            )

            df_exploded = df_exploded.withColumn(
                "standard_image", F.col("photo.url.standard")
            ).withColumn("main_photo_flag", F.col("photo.main_photo"))

            df_exploded = df_exploded.groupBy("id").agg(
                F.collect_list("standard_image").alias("images"),
                F.first(F.expr("standard_image"), ignorenulls=True).alias(
                    "default_image"
                ),  # First image as fallback
                F.first(
                    F.when(F.col("main_photo_flag") == True, F.col("standard_image")),
                    ignorenulls=True,
                ).alias(
                    "feature_image"
                ),  # Main photo as feature image
            )

            # Use default_image as feature_image if no main photo exists
            df_exploded = df_exploded.withColumn(
                "feature_image",
                F.coalesce(F.col("feature_image"), F.col("default_image")),
            ).drop("default_image")

            df_processed = df_processed.join(df_exploded, on="id", how="left")

        except Exception as error:
            logging.error(f"Error in process_hotel_photos: {error}")

        return df_processed

    def process_rooms_data(self, df_processed):
        try:
            df_exploded = df_processed.withColumn("room", F.explode(F.col("rooms")))

            df_rooms = df_exploded.select(
                F.col("id"),
                F.col("room.size").alias("room_size"),
                F.when(
                    F.col("room.maximum_occupancy").isNotNull(),
                    F.when(
                        F.col("room.maximum_occupancy.total_guests").isNotNull(),
                        F.col("room.maximum_occupancy.total_guests").cast("int"),
                    ).otherwise(
                        F.when(
                            F.col("room.maximum_occupancy.adults").isNotNull(),
                            F.col("room.maximum_occupancy.adults").cast("int"),
                        )
                    ),
                )
                .otherwise(0)
                .alias("max_occupancy"),
                F.col("room.number_of_rooms.bedrooms").alias("bedroom_count"),
                F.col("room.number_of_rooms.bathrooms").alias("bathroom_count"),
            ).fillna(
                {
                    "room_size": 0,
                    "max_occupancy": 0,
                    "bedroom_count": 0,
                    "bathroom_count": 0,
                }
            )

            df_rooms = df_rooms.withColumn(
                "room_size_sqft",
                F.round(F.col("room_size") * 10.76391, 2),
            ).withColumn(
                "bedroom_count",
                F.when(
                    F.col("bedroom_count") == 0, F.when(F.col("max_occupancy") < 5, 1)
                ).otherwise(F.col("bedroom_count")),
            )

            df_aggregated = df_rooms.groupBy("id").agg(
                F.collect_list("bedroom_count").alias("bedrooms_list"),
                F.sum("bedroom_count").alias("bedroom_count"),
                F.sum("bathroom_count").alias("bathroom_count"),
                F.sum("max_occupancy").alias("max_occupancy"),
                F.round(F.avg("room_size_sqft"), 2).alias("room_size_sqft"),
            )

            df_result = (
                df_aggregated.withColumn(
                    "bedrooms",
                    F.array_distinct(F.col("bedrooms_list")),
                )
                .withColumn("occupancy", F.col("max_occupancy"))
                .drop("bedrooms_list")
            )

            df_processed = df_processed.join(df_result, on="id", how="left")
        except Exception as error:
            logging.error(f"Error in process_rooms_data: {error}")

        return df_processed

    def process_property_flags_data(self, df_processed):
        REPLACE_PATTERN = r"[^a-zA-Z0-9\s]"
        PROPERTY_TYPE_CATEGORY = "Other"
        keyword_lists = get_keyword_lists()
    
        combined_columns = {
            "combined_4_column": ["property_name", "description.text.en-us", "property_type", "amenities"],
            "combined_3_column": ["property_name", "property_type", "amenities"],
            "combined_5_column": ["property_name", "description.text.en-us", "property_type", "amenities", "policy.other_policy"],
        }
        for col_name, cols in combined_columns.items():
            df_processed = df_processed.withColumn(
                col_name,
                F.concat_ws(" ", *[F.array_join(F.col(col), " ") if col == "amenities" else F.col(col) for col in cols])
            )
            df_processed = df_processed.withColumn(
                f"processed_{col_name}",
                F.lower(F.regexp_replace(F.col(col_name), REPLACE_PATTERN, ""))
            )
        
        df_processed = df_processed.withColumn(
            "processed_property_name",
            F.lower(F.regexp_replace(F.col("property_name"), REPLACE_PATTERN, ""))
        )
        
        for flag, keywords in keyword_lists.items():
            if flag == "is_all_inclusive_type":
                column_to_check = "processed_combined_5_column"
            elif flag == "is_hotel_type":
                column_to_check = "processed_combined_3_column"
            elif flag == "is_promoting_escort":
                column_to_check = "processed_property_name"
            else:
                column_to_check = "processed_combined_4_column"

            expr = F.lit(False)
            for keyword in keywords:
                expr |= F.col(column_to_check).contains(keyword)
            df_processed = df_processed.withColumn(flag, expr)
        
        df_processed = df_processed.withColumn(
            "is_business_travel",
            F.when(F.col("is_work_friendly"), True).otherwise(F.col("is_business_travel"))
        )
        df_processed = df_processed.withColumn(
            "is_pet_friendly",
            F.when(F.col("policy").isNotNull(), F.col("policy").getItem("is_pet_friendly")).otherwise(F.lit(False))
        )
        df_processed = df_processed.withColumn(
            "has_cancellation_policy",
            F.when(F.col("policy").isNotNull() & F.col("policy").getItem("cancellation_policy").isNotNull(), True).otherwise(F.lit(False))
        )
        df_processed = df_processed.withColumn(
            "is_other_type",
            F.when(F.col("property_type_category") == PROPERTY_TYPE_CATEGORY, True).otherwise(False)
        )
        df_processed = df_processed.withColumn(
            "adult_only",
            F.when(F.col("policy").isNotNull(), F.col("policy").getItem("adult_only")).otherwise(F.lit(False))
        )
        df_processed = df_processed.withColumn(
            "modified_is_preferred", 
            F.coalesce(F.col("is_preferred"), F.lit(False))
        )
        flag_columns = list(keyword_lists.keys()) + [
            "is_pet_friendly", "has_cancellation_policy", "is_other_type", "adult_only"
        ]
        df_processed = df_processed.withColumn(
            "property_flags",
            F.struct(
                *[F.col(flag).alias(flag) for flag in flag_columns],
                F.col("modified_is_preferred").alias("is_preferred")
            )
        )
        intermediate_columns = list(combined_columns.keys()) + [
            f"processed_{col}" for col in combined_columns.keys()
        ] + ["processed_property_name"] + flag_columns + ["modified_is_preferred"]
        df_processed = df_processed.drop(*intermediate_columns)
        
        return df_processed
    
    def after_process_property_flags_data(self, df_processed):
        REPLACE_PATTERN = r"[^a-zA-Z0-9\s]"
        PROPERTY_TYPE_CATEGORY = "Other"
        keyword_lists = get_keyword_lists()
        
        # Combine columns and process them in one go
        combined_columns = {
            "combined_4_column": ["property_name", "description.text.en-us", "property_type", "amenities"],
            "combined_3_column": ["property_name", "property_type", "amenities"],
            "combined_5_column": ["property_name", "description.text.en-us", "property_type", "amenities", "policy.other_policy"],
        }

        # Combine and process columns efficiently
        for col_name, cols in combined_columns.items():
            combined_col_expr = F.concat_ws(" ", *[
                F.array_join(F.col(col), " ") if col == "amenities" else F.col(col) 
                for col in cols
            ])
            df_processed = df_processed.withColumn(
                col_name, combined_col_expr
            ).withColumn(
                f"processed_{col_name}",
                F.lower(F.regexp_replace(F.col(col_name), REPLACE_PATTERN, ""))
            )
        
        # Process the property name column separately
        df_processed = df_processed.withColumn(
            "processed_property_name",
            F.lower(F.regexp_replace(F.col("property_name"), REPLACE_PATTERN, ""))
        )
        
        # Process flags efficiently by iterating over the keyword lists
        for flag, keywords in keyword_lists.items():
            column_to_check = {
                "is_all_inclusive_type": "processed_combined_5_column",
                "is_hotel_type": "processed_combined_3_column",
                "is_promoting_escort": "processed_property_name"
            }.get(flag, "processed_combined_4_column")
            
            expr = F.lit(False)
            for keyword in keywords:
                expr |= F.col(column_to_check).contains(keyword)
            
            df_processed = df_processed.withColumn(flag, expr)
        
        # Efficiently create or modify flag columns using `when`
        df_processed = df_processed.withColumn(
            "is_business_travel",
            F.when(F.col("is_work_friendly"), True).otherwise(F.col("is_business_travel"))
        ).withColumn(
            "is_pet_friendly",
            F.when(F.col("policy").isNotNull(), F.col("policy").getItem("is_pet_friendly")).otherwise(F.lit(False))
        ).withColumn(
            "has_cancellation_policy",
            F.when(F.col("policy").isNotNull() & F.col("policy").getItem("cancellation_policy").isNotNull(), True).otherwise(F.lit(False))
        ).withColumn(
            "is_other_type",
            F.when(F.col("property_type_category") == PROPERTY_TYPE_CATEGORY, True).otherwise(False)
        ).withColumn(
            "adult_only",
            F.when(F.col("policy").isNotNull(), F.col("policy").getItem("adult_only")).otherwise(F.lit(False))
        ).withColumn(
            "modified_is_preferred", 
            F.coalesce(F.col("is_preferred"), F.lit(False))
        )
        
        # Collect all flag column names in a list
        flag_columns = list(keyword_lists.keys()) + [
            "is_pet_friendly", "has_cancellation_policy", "is_other_type", "adult_only"
        ]
        
        # Create the "property_flags" struct column efficiently
        df_processed = df_processed.withColumn(
            "property_flags",
            F.struct(
                *[F.col(flag).alias(flag) for flag in flag_columns],
                F.col("modified_is_preferred").alias("is_preferred")
            )
        )
        
        # Drop intermediate columns to clean up the DataFrame
        intermediate_columns = list(combined_columns.keys()) + [
            f"processed_{col}" for col in combined_columns.keys()
        ] + ["processed_property_name"] + flag_columns + ["modified_is_preferred"]
        df_processed = df_processed.drop(*intermediate_columns)

        logging.info("===== : Optimized process Property Flags Data Complete : =====")
        
        return df_processed
 


    def process_commission_and_meal_plan_data(self, df_processed, df_search):
        df_search = df_search.withColumn(
            "id", F.concat(F.lit("BC-"), F.col("id").cast("string"))
        ).select("id", "commission", "products")

        df_processed = df_processed.join(df_search, on="id", how="left")
        df_processed = df_processed.withColumn(
            "meal_plan",
            F.when(
                (F.col("products.policies.meal_plan.meals").isNotNull()),
                F.array_distinct(
                    F.transform(
                        F.flatten(F.col("products.policies.meal_plan.meals")),
                        lambda x: F.initcap(x),
                    )
                ),
            ).otherwise(F.lit(None)),
        ).withColumn(
            "meal_plan",
            F.when(F.size(F.col("meal_plan")) > 0, F.col("meal_plan")).otherwise(
                F.lit(None)
            ),
        )
        return df_processed
    
    from pyspark.sql import functions as F

    def after_process_commission_and_meal_plan_data(self, df_processed, df_search):
        df_search = df_search.withColumn(
            "id", F.concat(F.lit("BC-"), F.col("id").cast("string"))
        ).select("id", "commission", "products")

        df_processed = df_processed.join(df_search, on="id", how="left").withColumn(
            "meal_plan",
            F.when(F.col("products.policies.meal_plan.meals").isNotNull(),
                F.array_distinct(
                    F.flatten(F.col("products.policies.meal_plan.meals"))
                ).alias("meal_plan"))
        )
        logging.info("===== : Optimized process Commission and Meal Plan Data Complete : =====")

        return df_processed


    """
    TODO:: Price is inserted only when the property is new,
    we need to update the price when the property is updated
    """
    def process_usd_price_and_price_history(
        self, df_processed, df_search, spark_session
    ):
        try:
            existing_price_df = None
            if (
                spark_session.catalog.tableExists("local.rental_property")
                and "usd_price"
                in spark_session.table("local.rental_property").schema.fieldNames()
            ):
                df_ids = df_processed.select("id")
                ids_list = [row["id"] for row in df_ids.collect()]
                ids_string = ", ".join([f"'{id_}'" for id_ in ids_list])

                del df_ids, ids_list

                query = f"""
                    SELECT id, feed_provider_id, usd_price
                    FROM local.rental_property
                    WHERE id IN ({ids_string})
                """
                del ids_string

                existing_price_df = spark_session.sql(query)

            if existing_price_df is None or existing_price_df.count() == 0:
                df_usd_price = df_search.select(
                    F.col("id").alias("feed_provider_id"),
                    F.abs(F.round(F.col("price.book") / 3, 2)).alias("usd_price"),
                ).fillna({"usd_price": 0})

                final_price_history_df = self.process_price_history(
                    df_processed=df_processed,
                    df_search=df_search,
                    for_first_time=True,
                )
            else:
                df_search_usd_price = df_search.select(
                    F.col("id").alias("feed_provider_id"),
                    F.abs(F.round(F.col("price.book") / 3, 2)).alias(
                        "search_usd_price"
                    ),
                ).fillna({"search_usd_price": 0})

                df_usd_price = (
                    existing_price_df.join(
                        df_search_usd_price, on="feed_provider_id", how="outer"
                    )
                    .withColumn(
                        "usd_price",
                        F.when(
                            ((F.col("usd_price").isNull()) | (F.col("usd_price") <= 0))
                            & (F.col("search_usd_price") >= 0),
                            F.col("search_usd_price"),
                        ).otherwise(F.col("usd_price")),
                    )
                    .withColumn(
                        "from_search_df",
                        F.when(
                            ((F.col("usd_price").isNull()) | (F.col("usd_price") <= 0))
                            & (F.col("search_usd_price") >= 0),
                            True,
                        ).otherwise(False),
                    )
                    .withColumn("usd_price", F.round(F.col("usd_price"), 2))
                    .select("feed_provider_id", "usd_price", "from_search_df")
                )

                final_price_history_df = self.process_price_history(
                    df_processed=df_processed,
                    df_search=df_search,
                    df_usd_price=df_usd_price,
                )

            df_usd_price = df_usd_price.drop("from_search_df")

            df_processed = df_processed.join(
                df_usd_price, on="feed_provider_id", how="left"
            ).fillna({"usd_price": 0})

        except Exception as error:
            logging.error(f"Error in process_usd_price: {error}")

        return df_processed, final_price_history_df

    def process_price_history(
        self,
        df_processed,
        df_search,
        df_usd_price=None,
        for_first_time=False,
    ):
        try:
            if for_first_time:
                df_price_history = (
                    df_search.select(
                        F.col("id").alias("feed_provider_id"),
                        F.col("price").alias("price"),
                        F.col("deep_link_url"),
                    )
                    .withColumn(
                        "checkin",
                        F.to_date(
                            F.regexp_extract(F.col("deep_link_url"), r"checkin=([\d-]+)", 1)
                        ),
                    )
                    .withColumn(
                        "checkout",
                        F.to_date(
                            F.regexp_extract(
                                F.col("deep_link_url"), r"checkout=([\d-]+)", 1
                            )
                        ),
                    )
                )
            else:
                df_search = df_search.withColumn("feed_provider_id", F.col("id"))
                df_price_history = (
                    df_usd_price.filter(F.col("from_search_df") == True)
                    .join(df_search, on="feed_provider_id", how="inner")
                    .select(
                        F.col("feed_provider_id"),
                        F.col("price"),
                        F.to_date(
                            F.regexp_extract(F.col("deep_link_url"), r"checkin=([\d-]+)", 1)
                        ).alias("checkin"),
                        F.to_date(
                            F.regexp_extract(F.col("deep_link_url"), r"checkout=([\d-]+)", 1)
                        ).alias("checkout"),
                    )
                )
                df_search = df_search.drop("feed_provider_id")

            final_price_history_df = df_processed.join(
                df_price_history, on="feed_provider_id", how="inner"
            )

            final_price_history_df = final_price_history_df.select(
                F.col("id").alias("property_id"),
                F.col("price"),
                F.col("checkin"),
                F.col("checkout"),
                F.col("feed"),
                F.col("country_code"),
            )

            final_price_history_df = final_price_history_df.withColumn(
                "id",
                F.concat(
                    F.col("property_id"),
                    F.lit("-"),
                    F.unix_timestamp(F.current_timestamp()),
                    F.lit("-"),
                    F.monotonically_increasing_id(),
                ),
            )
            return final_price_history_df
        except Exception as e:
            logging.error(f"Error in process_price_history: {e}")

    def process_reviews_data(self, df_final, df_reviews):
        exploded_df = df_reviews.withColumn("review",
                                            F.explode(F.col("reviews")))
        df_reviews = exploded_df.select(
            F.col("id").alias("feed_provider_id"),
            F.col("review.id").alias("review_id"),
            F.col("review.language").alias("language_code"),
            F.col("review.score").alias("score"),
            F.col("review.summary").alias("summary"),
            F.col("review.negative").alias("negative"),
            F.col("review.positive").alias("positive"),
            F.col("review.reviewer").alias("reviewer"),
            F.col("review.date").alias("review_date")
        )
        df_reviews = df_reviews.join(
            df_final,
            on="feed_provider_id",
            how="left"
        ).withColumn(
            "feed", F.lit(11)
        )
        df_reviews = df_reviews.withColumnRenamed("id", "property_id").withColumnRenamed("review_id", "id")
        df_reviews = df_reviews.filter(F.col("property_id").isNotNull())
        df_reviews = df_reviews.drop("feed_provider_id")

        return df_reviews

    def process_localize_data(self, df_processed, spark_session):
        df_processed = self.process_localize_language_code_and_locale_code(df_processed)
        df_processed = self.prepare_feature_summary(df_processed, spark_session)
        df_processed = self.process_localize_language_data(df_processed)
        df_processed = self.process_localize_property_name_description_and_policies_data(df_processed)
        df_processed = self.process_localize_property_slug_data(df_processed)
        df_processed = self.process_room_arrangement_data(df_processed, spark_session)
        df_processed = self.process_address_data(df_processed)
        df_processed = self.finalize_columns(df_processed)

        return df_processed

    def prepare_feature_summary(self, df_processed, spark_session):
        partner_facilities = self.broadcast.accommodation_facilities
        facility_types = self.broadcast.facility_types
        languages = ["en-us", "es", "fr", "de"]
 
        facilities_schema = F.StructType([
            T.StructField("facility_id", F.StringType(), True),
            T.StructField("facility_type_id", F.StringType(), True),
            T.StructField("language_code", F.StringType(), True),
            T.StructField("name", F.StringType(), True)
        ])
 
        facilities_rows = []
        for language in languages:
            facilities_rows.extend([
                (
                    facility_id,
                    str(facility_data.get("facility_type")),
                    language,
                    (facility_data.get("name", {}).get(language) or
                    facility_data.get("name", {}).get("fallback", "")).strip()
                )
                for facility_id, facility_data in partner_facilities.items()
                if (facility_data.get("name", {}).get(language) or
                    facility_data.get("name", {}).get("fallback", "")).strip()
            ])
 
        facilities_df = spark_session.createDataFrame(facilities_rows, facilities_schema)
 
        grouped_facilities = facilities_df.groupBy("facility_type_id", "language_code").agg(
            F.collect_set("name").alias("values"),
            F.collect_set("facility_id").alias("ids")
        )

        facility_types_rows = [
            (type_id, language, type_data["name"].get(language) or type_data["name"].get("fallback"))
            for type_id, type_data in facility_types.items()
            for language in languages
        ]
 
        facility_types_schema = F.StructType([
            T.StructField("facility_type_id", F.StringType(), True),
            T.StructField("language_code", F.StringType(), True),
            T.StructField("type_name", F.StringType(), True)
        ])
 
        facility_types_df = spark_session.createDataFrame(facility_types_rows, facility_types_schema)
 
        features_df = grouped_facilities.join(
            facility_types_df,
            ["facility_type_id", "language_code"]
        ).select(
            F.explode("ids").alias("facility_id"),
            "language_code",
            F.when(F.instr(F.col("type_name"), "_") > 0,
                F.regexp_replace(F.col("type_name"), "_", " ").cast("string"))
            .otherwise(F.col("type_name")).alias("fname"),
            F.concat_ws("\n", "values").alias("fdescription")
        )
 
        df_exploded = df_processed.withColumn(
            "exploded_facility",
            F.explode(F.expr("transform(facilities, x -> x.id)"))
        )
 
        df_result = (
            df_exploded.join(
                F.broadcast(features_df),
                (df_exploded["exploded_facility"] == features_df["facility_id"])
                & (df_exploded["language_code"] == features_df["language_code"]),
                "left",
            )
            .groupBy(df_exploded["id"], df_exploded["language_code"])
            .agg(
                F.array_distinct(
                    F.collect_list(
                        F.struct(
                            F.col("fname").alias("name"),
                            F.col("fdescription").alias("description"),
                        )
                    )
                ).alias("feature_summary")
            )
        )
 
        df_final = (
            df_processed.alias("processed")
            .join(
                df_result.alias("result"),
                (F.col("processed.id") == F.col("result.id"))
                & (F.col("processed.language_code") == F.col("result.language_code")),
                "left",
            )
            .select(
                F.col("processed.*"),
                F.col("result.feature_summary"),
            )
        )
 
        return df_final
    
    def process_address_data(self, df_processed):
        df_processed = df_processed.withColumn(
            "address",
            F.when(
                F.col("language_code") == "en-us",
                F.expr("location['address']['en-us']")
            )
            .when(
                F.col("language_code") == "fr",
                F.expr("location['address']['fr']")
            )
            .when(
                F.col("language_code") == "es",
                F.expr("location['address']['es']")
            )
            .when(
                F.col("language_code") == "de",
                F.expr("location['address']['de']")
            )
            .otherwise(
                F.expr("location['address']['en-us']")
            )
        ).withColumn(
            "address",
            F.regexp_replace(F.col("address"), "\x00", "")
        ).withColumn(
            "address",
            F.trim(F.col("address"))
        ).withColumn(
            "captions", F.lit("")
        ).withColumn(
            "area_description", F.lit("")
        ).withColumn(
            "is_translated", F.lit(True)
        ).withColumn(
            "display", F.lit("")
        )

        return df_processed

    def process_localize_language_code_and_locale_code(self, df_processed):
        df_processed = df_processed.withColumn("language_code", F.explode("languages"))
        df_processed = df_processed.withColumn(
            "locale_code",
            F.when(F.col("language_code").isin("en-us", "en-gb"), "en").otherwise(F.col("language_code")),
        )
        return df_processed
    
    def process_localize_language_data(self, df_processed):
        locale_data = self.broadcast.locales_data
        locale_col = F.col("locale_code")
        conditions = [
            (locale_col == localize_info.get("code"), language)
            for language, localize_info in locale_data.items()
            if not localize_info.get("disabled")
        ]

        condition = F.coalesce(*[F.when(cond, lang) for cond, lang in conditions])
        df_processed = df_processed.withColumn("language", condition)

        return df_processed
    
    def process_localize_property_name_description_and_policies_data(self, df_processed):
        languages = (
            df_processed.selectExpr("explode(languages) as lang")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        existing_fields = df_processed.schema["name"].dataType.names
        valid_languages = [lang for lang in languages if lang in existing_fields]
        name_map_expr = sum(
            [[lit(lang), col(f"name.{lang}")] for lang in valid_languages], []
        )

        df_processed = df_processed.withColumn("name_map", create_map(*name_map_expr))
        df_processed = df_processed.withColumn(
            "localize_property_name",
            F.when(F.col("locale_code").isin("en"), F.col("property_name")).otherwise(
                F.col("name_map").getItem(F.col("locale_code"))
            ),
        )

        # Description 
        description_map_expr = sum(
            [[lit(lang), col(f"description.text.{lang}")] for lang in valid_languages],
            [],
        )
        df_processed = df_processed.withColumn(
            "description_map", create_map(*description_map_expr)
        )
        df_processed = df_processed.withColumn(
            "localize_property_description",
            F.col("description_map").getItem(F.col("language_code")),
        ).withColumn(
            "localize_property_description",
            F.when(
                F.col("localize_property_description").isNotNull(),
                F.regexp_replace(F.col("localize_property_description"), "\x00", ""),
            ).otherwise(F.col("localize_property_description")),
        )

        # Policies
        description_important_information_map_expr = sum(
            [[lit(lang), col(f"description.important_information.{lang}")] for lang in valid_languages],
            [],
        )
        df_processed = df_processed.withColumn(
            "description_information_map",
            create_map(*description_important_information_map_expr)
        )

        # other policy data as a column 
        df_processed = df_processed.withColumn(
            "other_policy",
            F.coalesce(
                F.col("description_information_map").getItem(F.col("language_code")),
                F.lit(None)
            )
        )
        policy_columns = df_processed.select("policy.*").columns
        df_processed = df_processed.withColumn(
            "existing_policies",
            F.when(
                F.col("other_policy").isNotNull(),
                F.struct(
                    F.col("other_policy").alias("other_policy"),
                    *[
                        F.col(f"policy.{col_name}")
                        for col_name in policy_columns if col_name != "other_policy"
                    ]
                )
            ).otherwise(
                F.struct(
                    *[
                        F.col(f"policy.{col_name}")
                        for col_name in policy_columns
                    ]
                )
            )
        )
        df_processed = df_processed.withColumn(
            "new_policies",
            when(
                F.col("other_policy").isNotNull(),
                F.struct(F.col("other_policy").alias("other_policy"))
            ).otherwise(F.lit(None))
        )

        df_processed = df_processed.withColumn(
            "localize_policies",
            when(
                F.col("locale_code") == 'en',
                F.col("existing_policies").cast("string")
            ).otherwise(F.col("new_policies").cast("string"))
        )

        return df_processed
    
    def process_localize_property_slug_data(self, df_processed):
        df_processed = df_processed.withColumn(
            "localize_property_slug",
            F.lower(
                F.regexp_replace(
                    F.trim(F.col("localize_property_name")), r"[^a-zA-Z0-9]+", "-"
                )
            ),
        ).withColumn("updated_at", F.lit(datetime.now().date()))

        return df_processed

    def process_room_arrangement_data(self, df_processed, spark_session):
        facility_types = self.broadcast.facility_types
        bed_types = self.broadcast.bed_types
 
        # Creating DataFrames
        facility_types_df = (
            spark_session.createDataFrame(
                [
                    (
                        key,
                        value["name"].get("en-us"),
                        value["name"].get("fr"),
                        value["name"].get("es"),
                        value["name"].get("de"),
                    )
                    for key, value in facility_types.items()
                ],
                schema=["room_type_id", "en_us", "fr", "es", "de"],
            )
            .withColumnRenamed("en_us", "facility_en_us")
            .withColumnRenamed("fr", "facility_fr")
            .withColumnRenamed("es", "facility_es")
            .withColumnRenamed("de", "facility_de")
        )
 
        bed_types_df = (
            spark_session.createDataFrame(
                [
                    (
                        key,
                        value["name"].get("en-us"),
                        value["name"].get("fr"),
                        value["name"].get("es"),
                        value["name"].get("de"),
                    )
                    for key, value in bed_types.items()
                ],
                schema=["bed_type_id", "en_us", "fr", "es", "de"],
            )
            .withColumnRenamed("en_us", "bed_en_us")
            .withColumnRenamed("fr", "bed_fr")
            .withColumnRenamed("es", "bed_es")
            .withColumnRenamed("de", "bed_de")
        )
 
        # Explode rooms column data
        df_exploded = df_processed.withColumn(
            "exploded_rooms", F.explode(F.col("rooms"))
        )

        # room id and room name
        df_exploded = df_exploded.withColumn(
            "room_id", F.col("exploded_rooms.id")
        ).withColumn(
            "room_name",
            F.when(
                F.col("language_code") == "en-us",
                F.col("exploded_rooms.name.en-us"),
            )
            .when(F.col("language_code") == "fr", F.col("exploded_rooms.name.fr"))
            .when(F.col("language_code") == "es", F.col("exploded_rooms.name.es"))
            .when(F.col("language_code") == "de", F.col("exploded_rooms.name.de"))
            .otherwise(F.lit(None)),
        )
 
        # Room type
        df_exploded = df_exploded.join(
            facility_types_df,
            df_exploded["exploded_rooms.room_type"]
            == facility_types_df["room_type_id"],
            "left",
        )
 
        df_exploded = df_exploded.withColumn(
            "exploded_room_room_type",
            F.when(F.col("language_code") == "en-us", F.col("facility_en_us"))
            .when(F.col("language_code") == "fr", F.col("facility_fr"))
            .when(F.col("language_code") == "es", F.col("facility_es"))
            .when(F.col("language_code") == "de", F.col("facility_de"))
            .otherwise(F.col("facility_en_us")),
        )
 
        # Explode bed_options
        df_bed_options = df_exploded.withColumn(
            "exploded_bed_options", F.explode(F.col("exploded_rooms.bed_options"))
        )
 
        df_bed_configurations = df_bed_options.withColumn(
            "exploded_bed_configurations", F.explode(F.col("exploded_bed_options.bed_configurations"))
        )
 
        df_bed_configuration_details = df_bed_configurations.withColumn(
            "exploded_configuration", F.explode(F.col("exploded_bed_configurations.configuration"))
        ).withColumn(
            "bed_type_id", F.col("exploded_configuration.bed_type")
        ).withColumn(
            "number_of_beds", F.col("exploded_configuration.number_of_beds")
        )
        
        df_bed_configuration_details = df_bed_configuration_details.join(
            bed_types_df,
            df_bed_configuration_details["bed_type_id"] == bed_types_df["bed_type_id"],
            "left",
        ).withColumn(
            "bed_type",
            F.coalesce(
                F.when(F.col("language_code") == "en-us", F.col("bed_en_us"))
                .when(F.col("language_code") == "fr", F.col("bed_fr"))
                .when(F.col("language_code") == "es", F.col("bed_es"))
                .when(F.col("language_code") == "de", F.col("bed_de")),
                F.col("bed_en_us")
            )
        )
        df_bed_options_grouped = df_bed_configuration_details.groupBy("id").agg(
            F.array_distinct(
                F.collect_list(
                    F.struct(
                        F.col("bed_type").alias("bed_type"),
                        F.col("number_of_beds").alias("number_of_beds")
                    )
                )
            ).alias("configuration"),
            F.first(F.col("exploded_rooms.cribs_and_extra_beds.are_allowed"), ignorenulls=True).alias("are_allowed"),
        )
        df_bed_options_grouped = df_bed_options_grouped.withColumn(
            "bed_options",
            F.struct(
                F.col("configuration").alias("configuration"),
                F.col("are_allowed").alias("cribs_and_extra_beds")
            )
        ).select("id", "bed_options")

 
        df_exploded = df_exploded.join(df_bed_options_grouped, on="id", how="left")
        
        # Process Size
        df_exploded = df_exploded.withColumn(
            "size",
            when(
                F.col("exploded_rooms.size").isNotNull(),
                F.col("exploded_rooms.size")
            ).otherwise(F.lit(0))
        ).withColumn(
            "size", F.col("size").cast("string")
        )

        # For process photos, number_of_rooms, maximum_occupancy, attributes
        df_exploded = df_exploded.withColumn(
            "room_photos",
            F.when(
                F.col("locale_code").isin(["en", "en-us"]),
                F.expr("TRANSFORM(photos, x -> x['url']['standard'])")
            ).otherwise(F.lit(None))
        )

        df_exploded = df_exploded.withColumn(
            "room_attributes", F.lit([]).cast(T.ArrayType(T.StringType()))
        ).withColumn(
            "room_attributes",
            F.when(
                F.col("locale_code").isin(["en", "en-us"]),
                F.col("exploded_rooms.attributes")
            )
        )
        

        df_exploded = df_exploded.withColumn(
            "exploded_room_number_of_rooms",
            F.when(
                F.col("locale_code").isin(["en", "en-us"]),
                F.struct(
                    F.col("exploded_rooms.number_of_rooms.bedrooms").cast("string").alias("bedrooms"),
                    F.col("exploded_rooms.number_of_rooms.bathrooms").cast("string").alias("bathrooms"),
                    F.col("exploded_rooms.number_of_rooms.living_rooms").cast("string").alias("living_rooms"),
                )
            ).otherwise(
                F.struct(
                    F.lit(None).cast("string").alias("bedrooms"),
                    F.lit(None).cast("string").alias("bathrooms"),
                    F.lit(None).cast("string").alias("living_rooms"),
                )
            )
        )
        df_exploded = df_exploded.withColumn(
            "exploded_room_maximum_occupancy",
            F.when(
                F.col("locale_code").isin(["en", "en-us"]),
                F.struct(
                    F.col("exploded_rooms.maximum_occupancy.adults").cast("string").alias("adults"),
                    F.col("exploded_rooms.maximum_occupancy.children").cast("string").alias("children"),
                    F.col("exploded_rooms.maximum_occupancy.total_guests").cast("string").alias("total_guests"),
                )
            ).otherwise(
                F.struct(
                    F.lit(None).cast("string").alias("adults"),
                    F.lit(None).cast("string").alias("children"),
                    F.lit(None).cast("string").alias("total_guests"),
                )
            )
        )
        # TODO: To reduce execution time, the feature summary for room arrangements is not included initially. It will be processed later during the data synchronization to S3 and OpenSearch.
        df_room_arrangement = df_exploded.withColumn(
            "room_arrangement",
            F.struct(
                F.col("room_id").cast("string").alias("id"),
                F.col("room_name").alias("name"),
                F.col("room_photos").alias("photos"),
                F.col("room_attributes").alias("attributes"),
                # F.col("feature_summary").alias("feature_summary"),
                F.col("exploded_room_room_type").alias("room_type"),
                F.col("bed_options"),
                F.col("exploded_room_number_of_rooms").alias("number_of_rooms"),
                F.col("exploded_room_maximum_occupancy").alias("maximum_occupancy"),
                F.col("size"),
            )
        )
 
        df_room_arrangement_grouped = df_room_arrangement.groupBy("id").agg(
            F.collect_list("room_arrangement").alias("room_arrangement_arr_struct")
        )
 
        df_final = df_processed.join(df_room_arrangement_grouped, on="id", how="left")

        return df_final
    
    def finalize_columns(self, df_processed):
        df_processed = df_processed.select(
            F.col("id").alias("property_id").cast("string"),
            F.col("feed").alias("feed"),
            F.col("localize_property_name").alias("property_name"),
            F.col("property_type"),
            F.col("country_code"),
            F.col("language"),
            F.col("localize_property_slug").alias("property_slug"),
            F.col("localize_property_description").alias("property_description"),
            F.col("localize_policies").alias("policies"),
            F.col("room_arrangement_arr_struct").alias("room_arrangement"),
            F.col("updated_at"),
            F.col("address"),
            F.col("captions"),
            F.col("area_description"),
            F.col("is_translated"),
            F.col("feature_summary"),
            F.col("display"),
            # F.col("created_at")
        )

        return df_processed

