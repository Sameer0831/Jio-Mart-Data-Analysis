from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.DataWrite.DataBaseWrite import *

#calculation for customer mart
#find out the customer total purchase every month
#write the data into MySQL table
def customer_mart_calculation_table_write(final_customer_data_mart_df):
    window = Window.partitionBy("customer_id","sales_date_month")
    final_customer_data_mart = final_customer_data_mart_df.withColumn("sales_date_month",
                                           substring(col("sales_date"),1,7))\
                    .withColumn("total_sales_every_month_by_each_customer",
                                sum("total_cost").over(window))\
                    .select("customer_id", concat(col("first_name"),lit(" "),col("last_name"))
                            .alias("full_name"),"address","phone_number",
                            "sales_date_month",
                            col("total_sales_every_month_by_each_customer").alias("total_sales"))\
                    .distinct()

    """
    If this script is throwing some date related issues within sql then use these 2 solutions
     1. run the below script: 
            we modified the date as "yyyy-MM-01" temporarily.
            
            # Modify sales_date_month to be the first day of the month
            final_customer_data_mart = final_customer_data_mart_df.withColumn(
                "sales_date_month", date_format(col("sales_date"), "yyyy-MM-01")
            ).withColumn(
                "total_sales_every_month_by_each_customer",
                sum("total_cost").over(window)
            ).select(
                "customer_id",
                concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
                "address",
                "phone_number",
                "sales_date_month",
                col("total_sales_every_month_by_each_customer").alias("total_sales")
            ).distinct()
    2. update the date column with varchar() or char() to utilize the substring function
            Syntax:
            =======
                    ALTER TABLE your_table_name (customers_data_mart)
                    MODIFY COLUMN sales_date_month VARCHAR(7);

    """

    final_customer_data_mart.show()

    #Write the Data into MySQL customers_data_mart table
    db_writer = DatabaseWriter(url=config.url,properties=config.properties)
    db_writer.write_dataframe(final_customer_data_mart,config.customer_data_mart_table)

