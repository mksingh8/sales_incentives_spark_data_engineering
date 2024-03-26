from pyspark.sql import Window
from pyspark.sql.functions import substring, col


def sales_mart_calculation_table_write(final_sales_team_data_mart_df):
    window = Window.partitionBy("store_id", "sales_person_id", "sales_month")
    final_sales_team_data_mart = (final_sales_team_data_mart_df
                                  .withColumn("sales_month", substring(col("sales_month"), 1, 7))
                                  .withColumn("total_sales_every_month", sum(col("")))

                                  )