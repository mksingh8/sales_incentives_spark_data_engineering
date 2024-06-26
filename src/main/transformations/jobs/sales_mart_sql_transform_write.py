# from pyspark.sql import Window
# from pyspark.sql.functions import col, substring, lit, concat, rank, when
#
# from resources.dev import config
# from src.main.write.database_write import DatabaseWriter
#
#
# def sales_mart_calculation_table_write(final_sales_team_data_mart_df):
#     window = Window.partitionBy("store_id", "sales_person_id", "sales_month")
#     final_sales_team_data_mart = final_sales_team_data_mart_df \
#         .withColumn("total_sales_every_month",
#                     sum(col("total_cost")).over(window)) \
#         .select("store_id", "sales_person_id", concat(col("sales_person_first_name"),
#                                                       lit(" "), col("sales_person_last_name"))
#                 .alias("full_name"), "sales_month", "total_sales_every_month") \
#         .distinct()
#     rank_window = Window.partitionBy("store_id", "sales_month").orderBy(col("total_sales_every_month").desc())
#     final_sales_team_data_mart_table = final_sales_team_data_mart.withColumn("rnk", rank().over(rank_window))\
#         .withColumn("incentive", when(col("rnk") ==1,
#                                       col("total_sales_every_month") * 0.01).otherwise(lit(0)))\
#         .withColumn("incentive", round(col("incentive"), 2 ))\
#         .withColumn("total_sales", col("total_sales_every_month"))\
#         .select("store_id", "sales_person_id", "full_name", "sales_month", "total_sales", "incentive")
#
#     # Write the data into mysql sales teams data mart table
#     print("Writing the data into sales team data mart")
#     db_writer = DatabaseWriter(config.url, config.properties)
#     db_writer.write_dataframe(final_sales_team_data_mart_table, config.sales_team_data_mart_table)


from pyspark.sql import functions as F
from pyspark.sql.window import Window

from resources.dev import config
from src.main.write.database_write import DatabaseWriter


def sales_mart_calculation_table_write(final_sales_team_data_mart_df):
    window = Window.partitionBy("store_id", "sales_person_id", "sales_month")
    # Correctly use sum within the over window function
    final_sales_team_data_mart = final_sales_team_data_mart_df \
        .withColumn("total_sales_every_month",
                    F.sum("total_cost").over(window)) \
        .select("store_id", "sales_person_id", F.concat(F.col("sales_person_first_name"),
                                                        F.lit(" "), F.col("sales_person_last_name"))
                .alias("full_name"), "sales_month", "total_sales_every_month") \
        .distinct()
    rank_window = Window.partitionBy("store_id", "sales_month").orderBy(F.col("total_sales_every_month").desc())
    final_sales_team_data_mart_table = final_sales_team_data_mart.withColumn("rnk", F.rank().over(rank_window)) \
        .withColumn("incentive", F.when(F.col("rnk") == 1,
                                        F.col("total_sales_every_month") * 0.01).otherwise(F.lit(0))) \
        .withColumn("incentive", F.round(F.col("incentive"), 2)) \
        .withColumn("total_sales", F.col("total_sales_every_month")) \
        .select("store_id", "sales_person_id", "full_name", "sales_month", "total_sales", "incentive")

    # Write the data into mysql sales teams data mart table
    print("Writing the data into sales team data mart")
    db_writer = DatabaseWriter(config.url, config.properties)
    db_writer.write_dataframe(final_sales_team_data_mart_table, config.sales_team_data_mart_table)
