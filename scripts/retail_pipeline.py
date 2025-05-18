#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql.functions import year, month, concat_ws, broadcast, sum, col, avg, count


# In[2]:


#Create a Spark Session
#Define Schemas for our dataFrame based on the csv file we have stored in Google Cloud Storage Bucket.

spark = SparkSession.builder\
        .appName("Retail DataSet")\
        .getOrCreate()


# In[3]:


#Reading the data from Cloud Storage Bucket and creating dataFrame


# In[3]:


customers_df = spark.read\
        .option("header", True)\
        .option("mode", "PERMISSIVE")\
        .option("badRecordsPath", "gs://retail_pysparkdata/bad_data")\
        .option("nullValue", "")\
        .option("inferSchema", True)\
        .csv("gs://retail_pysparkdata/customers.csv")

transactions_df = spark.read\
            .option("header", True)\
            .option("mode", "PERMISSIVE")\
            .option("badRecordsPath", "gs://retail_pysparkdata/bad_data")\
            .option("nullValue", "")\
            .option("inferSchema", True)\
            .csv("gs://retail_pysparkdata/transactions.csv")


# In[5]:


#Clean data - nulls & remove duplicates


# In[6]:


customers_df_clean = customers_df.dropna(subset = ["customer_id"]).dropDuplicates(["customer_id"])


# In[7]:


transactions_df_clean = transactions_df.dropna(subset = ["customer_id","transaction_id","quantity","price"]).dropDuplicates(["customer_id"])


# In[8]:


#Add transaction year/month and full address new column

transactions_df_enriched = transactions_df_clean.withColumn("transaction_year", year("transaction_date"))\
                            .withColumn("transaction_month", month("transaction_date"))

customers_df_enriched = customers_df_clean.withColumn("full_address", concat_ws(", ", "street_address", "city", "state", "zip_code"))


# In[9]:


customers_count = transactions_df_enriched.groupBy("customer_id").agg(count(col("transaction_id")).alias("count_cust"))


# In[10]:


customers_count = transactions_df_enriched.groupBy("customer_id").agg(count(col("transaction_id")).alias("count_cust"))
customers_count.filter("count_cust > 1").show()


# In[11]:


transactions_df_enriched.printSchema()


# In[9]:


#Lets cache the dataframe thats enriched and data handled for reuse


# In[12]:


# Repartition by customer_id for better join performance
customers_df_enriched = customers_df_enriched.repartition("customer_id")
transactions_df_enriched = transactions_df_enriched.repartition("customer_id")

# Cache for reuse
customers_df_enriched.cache()
transactions_df_enriched.cache()


# In[12]:


#Lets Explore Joins


# In[13]:


#Inner Join

inner_join_df = transactions_df_enriched.join(customers_df_enriched, how="inner", on="customer_id")


# In[14]:


#Left Outer Join

left_outer_join_df = transactions_df_enriched.join(customers_df_enriched, how="left", on="customer_id")


# In[15]:


#Right Outer Join

right_outer_join_df = transactions_df_enriched.join(customers_df_enriched, how="right", on="customer_id")


# In[16]:


#Full Outer Join

full_outer_join_df = transactions_df_enriched.join(customers_df_enriched, how="full", on="customer_id")


# In[17]:


#Broadcast Join

broadcast_join_df =transactions_df_enriched.join(broadcast(customers_df_enriched), how="outer", on="customer_id")


# In[18]:


#Common Aggregations & Transformations


# In[18]:


#To Find Total Spend per customer, lets first create a amount column with price & quantity and then apply the discount percentage

#transactions_df_enriched = transactions_df_enriched.withColumn("amount", col("quantity") * col("price"))

#transactions_df_enriched = transactions_df_enriched.withColumn("final_amount", col("amount") - col("discount_applied"))


#We found NULL being reported for most final_amount - Lets Investigate

transactions_df_enriched.select("quantity", "price", "discount_applied")\
                        .where(col("quantity").isNull() | col("price").isNull() | col("discount_applied").isNull())\
                        .show()

#It is now investigated that in the dataset there is Quqntity value blank when price is present thus it cascades null to final_amount


# In[19]:


#It is now investigated that in the dataset there is Quqntity value blank when price is present thus it cascades null to final_amount
#Lets create safe columns without NUll before calculating Final Amount

#We tried coalesce but that was replacing all with 0.0 because quantity is blank and not null. So alrernate method below

from pyspark.sql.functions import col, when, lit

# Replace empty strings or nulls with 0 for quantity and price
transactions_df_enriched = transactions_df_enriched\
                        .withColumn("clean_quantity", when((col("quantity").isNull()) | (col("quantity") == ""), lit(0))\
                                    .otherwise(col("quantity").cast("int")))\
                        .withColumn("clean_price", when((col("price").isNull()) | (col("price") == ""), lit(0.0))\
                                    .otherwise(col("price").cast("double")))\
                        .withColumn("clean_discount", when((col("discount_applied").isNull()) | (col("discount_applied") == ""), lit(0))\
                                    .otherwise(col("discount_applied").cast("int")))\
                        .withColumn("amount", col("clean_quantity") * col("clean_price"))\
                        .withColumn("final_amount", col("amount") - col("clean_discount"))



# In[20]:


#Now that we got the final columns - final_amount / amount we can drop the safe columns we created.

transactions_df_enriched = transactions_df_enriched.drop("clean_quantity","clean_price","clean_discount")


# In[21]:


#To Find Total Spend per customer

total_spend_df = transactions_df_enriched.groupBy("customer_id").agg(sum("final_amount").alias("total_spend"))


# In[22]:


#Average Spend Per Store

average_spend_df = transactions_df_enriched.groupBy("store_location").agg(avg("final_amount").alias("avg_spend"))


# In[23]:


#Count of Transactions Per Payment Method

count_trans_paymentMethod = transactions_df_enriched.groupBy("payment_method").agg(count("transaction_id"))


# In[24]:


#High-Value Transactions (Final Amount > 1000)

transactions_highvalue_df = transactions_df_enriched.filter("final_amount > 1000")


# In[25]:


#Top N Customers by Spend

top_customer_df = total_spend_df.orderBy(col("total_spend").desc()).limit(10)



# In[90]:


# Count of Transactions per City - this may not give accurate result

#trans_city_df = customers_df_enriched.join(transactions_df_enriched, on="customer_id", how = "left").groupBy("city").count()


# In[26]:


# Count of Transactions per City

trans_city_df = customers_df_enriched.join(transactions_df_enriched, on="customer_id", how = "left").groupBy("city").agg(count(when(col("transaction_id").isNotNull(),1)).alias("transaction_count"))


# In[27]:


#Average Spend per Month

avg_spend_month = transactions_df_enriched.groupBy("transaction_month").agg(avg("final_amount").alias("avg_spend_month"))


# In[35]:


#Rename & Drop Columns
renamed_df = transactions_df_enriched.withColumnRenamed("total_amount" , "final_amount")

transactions_df_enriched = renamed_df.drop("total_amount")




# In[28]:


transactions_df_enriched.show()


# In[29]:


#Adding Derived Columns (withColumn)

from pyspark.sql.functions import when

transactions_flagged_df = transactions_df_enriched.withColumn("high_value_flag",when(transactions_df_enriched["amount"] > 1000, "HIGH")\
                                                                .otherwise("NORMAL"))


# In[30]:


#Select and Rename Columns

renamed_df = transactions_flagged_df.selectExpr("transaction_id as txn_id","customer_id","amount","high_value_flag")


# In[31]:


renamed_df.show()


# In[ ]:


# Repartition and Cache (optional for performance tuning)


# In[33]:


from pyspark.sql.functions import lit, monotonically_increasing_id
from pyspark.sql import DataFrame

# 1. Pick a sample of customers to duplicate
sample_customers = transactions_df_enriched.select("customer_id").distinct().limit(5)




# In[34]:


# 2. Join with transactions to get their original transactions
sample_txns = sample_customers.join(transactions_df_enriched, on="customer_id", how="inner")



# In[35]:


#WINDOW Functions..

#Rank by amount per customer

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum as _sum, lag, lead

window_spec_rank = Window.partitionBy("customer_id").orderBy(col("final_amount").desc())

ranked_products_df = transactions_df_enriched.withColumn("rank_in_customer",row_number().over(window_spec_rank))


# In[37]:


transactions_df_enriched.select("customer_id","amount","final_amount").show()


# In[38]:


ranked_products_df.select("customer_id","final_amount","rank_in_customer").show()


# In[39]:


#Running total of final_amount per customer

window_spec_running = Window.partitionBy("customer_id").orderBy("transaction_date")

running_total_df = transactions_df_enriched.withColumn("running_total",_sum("final_amount").over(window_spec_running))


# In[40]:


#Compare each transaction with previous

window_spec_lag = Window.partitionBy("customer_id").orderBy("transaction_date")

lag_lead_df = transactions_df_enriched.withColumn("prev_amount",lag("final_amount", 1).over(window_spec_lag))\
            .withColumn("next_amount",lead("final_amount", 1).over(window_spec_lag))


# In[41]:


#Dense Rank: Cities by total spend

city_total_df = customers_df_enriched.join(transactions_df_enriched, "customer_id")

city_spend_df = city_total_df.groupBy("city").agg(
    _sum("final_amount").alias("total_spent")
)

window_spec_city = Window.orderBy(col("total_spent").desc())

city_ranked_df = city_spend_df.withColumn(
    "dense_rank", dense_rank().over(window_spec_city)
)


# In[42]:


print("Before repartition:", transactions_df_enriched.rdd.getNumPartitions())


# In[43]:


transactions_df_repartitioned = transactions_df_enriched.repartition(10, "customer_id")


# In[44]:


print("After repartition:", transactions_df_repartitioned.rdd.getNumPartitions())


# In[62]:


"""Performance Tuning...!!!!!
Step 8: Repartition, Coalesce, and Caching
These are critical concepts when optimizing performance in PySpark — especially for large datasets.
8.1 Repartitioning
When to use?
Increase parallelism when working with larger datasets
Ensures better load balancing across Spark executors

# Check current number of partitions
print("Before repartition:", transactions_df_with_dupes.rdd.getNumPartitions())  --> 200

# Repartition to 10 partitions based on customer_id
transactions_df_repartitioned = transactions_df_with_dupes.repartition(10, "customer_id")

print("After repartition:", transactions_df_repartitioned.rdd.getNumPartitions())
✅ 8.2 Coalesce
When to use?
Reduce number of partitions

Especially useful before writing to disk or performing actions like collect()
# Coalesce into fewer partitions (e.g., 2) before saving
transactions_df_coalesced = transactions_df_repartitioned.coalesce(2)
✅ 8.3 Caching / Persisting
When to use?
If you're using a DataFrame multiple times in your pipeline

# Cache to memory
transactions_df_repartitioned.cache()
transactions_df_repartitioned.count()  # Trigger cache
or

from pyspark.storagelevel import StorageLevel

# Persist to memory and disk
transactions_df_repartitioned.persist(StorageLevel.MEMORY_AND_DISK)

"""


# In[45]:


transactions_df_coalesced = transactions_df_repartitioned.coalesce(2)


# In[46]:


print("After Coalesce:", transactions_df_coalesced.rdd.getNumPartitions())


# In[47]:


#Save output as csv

transactions_df_repartitioned \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("gs://retail_pysparkdata//output/transactions")


# In[48]:


#Save output as parque

transactions_df_repartitioned \
    .write \
    .mode("overwrite") \
    .parquet("gs://retail_pysparkdata//output/transactionss_parquet")


# In[49]:


spark.stop()


# In[ ]:




