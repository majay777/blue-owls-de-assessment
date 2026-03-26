-- --For each product category, calculate monthly revenue (price + freight_value) and rank categories within each month by ----revenue. Then for the top 5 categories by overall revenue, show their month-over-month revenue growth percentage and a ---3-month rolling average of revenue. Only include months where the category had at least 10 transactions.

-- from pyspark.sql import functions as F
-- from pyspark.sql.window import Window

-- # 1. Monthly Aggregation with Transaction Filter
-- monthly_stats = fact_order_items.groupBy(
--     "product_category_name", 
--     F.trunc("order_date", "MM").alias("month")
-- ).agg(
--     F.sum(F.col("price") + F.col("freight_value")).alias("monthly_revenue"),
--     F.count("order_id").alias("transaction_count")
-- ).filter(F.col("transaction_count") >= 10) # Requirement: Min 10 transactions

-- # 2. Rank Categories within each month by revenue
-- month_window = Window.partitionBy("month").orderBy(F.desc("monthly_revenue"))
-- monthly_ranked = monthly_stats.withColumn("monthly_rank", F.rank().over(month_window))

-- # 3. Identify the Top 5 Categories by OVERALL total revenue
-- overall_top_5 = monthly_stats.groupBy("product_category_name") \
--     .agg(F.sum("monthly_revenue").alias("total_overall_revenue")) \
--     .orderBy(F.desc("total_overall_revenue")) \
--     .limit(5)

-- # 4. Filter monthly data for only these Top 5 and apply Window Metrics
-- # Join keeps only the top 5 categories
-- top_5_monthly = monthly_ranked.join(overall_top_5, on="product_category_name")

-- category_window = Window.partitionBy("product_category_name").orderBy("month")
-- rolling_window = category_window.rowsBetween(-2, 0) # Current + 2 previous months

-- final_report = top_5_monthly.withColumn(
--     "prev_month_revenue", F.lag("monthly_revenue").over(category_window)
-- ).withColumn(
--     "mom_growth_pct", 
--     ((F.col("monthly_revenue") - F.col("prev_month_revenue")) / F.col("prev_month_revenue") * 100)
-- ).withColumn(
--     "3_month_rolling_avg", F.avg("monthly_revenue").over(rolling_window)
-- ).select(
--     "month", "product_category_name", "monthly_revenue", 
--     "monthly_rank", "mom_growth_pct", "3_month_rolling_avg"
-- ).orderBy("month", "monthly_rank")

-- # Save to Gold
-- final_report.write.mode("overwrite").csv("gold/top_category_performance", header=True)
