from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter


# calculation for customer mart
# find out the customer total purchase every month
# write the data into MySQL table
def customer_mart_calculation_table_write(final_customer_data_mart_df):
    window = Window.partitionBy("customer_id", "sales_date_month")
    final_customer_data_mart = final_customer_data_mart_df.withColumn("sales_date_month",
                                                                      substring(col("sales_date"), 1, 7)) \
        .withColumn("total_sales_every_month_by_each_customer",
                    sum("total_cost").over(window)) \
        .select("customer_id", concat(col("first_name"), lit(" "), col("last_name"))
                .alias("full_name"), "address", "phone_number",
                "sales_date_month",
                col("total_sales_every_month_by_each_customer").alias("total_sales")) \
        .distinct()

    # Writing the dataframe in mysql table, customers_data_mart was failing because customers_data_mart column is
    # expecting value in valid date format but the dataframe has only year and month. one way to fix this is to change
    # the column properties to varchar. However, this approach might not be suitable if you need to perform
    # date-specific operations on this column in MySQL.
    # Another way is to match full date format by adding any lit value to the date. Say -01.
    final_customer_data_mart = final_customer_data_mart.withColumn("sales_date_month",
                                                                   concat(col("sales_date_month"), lit("-01")))
    final_customer_data_mart.show()

    # Write the Data into MySQL customers_data_mart table
    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(final_customer_data_mart, config.customer_data_mart_table)
