import org.apache.spark.sql.functions._

// 1. Load ALL the required CSV files directly from HDFS
val weblogRaw = spark.read.option("header", "false").csv("/input/weblog/weblog.csv")
  .toDF("ip", "datetime", "dept", "category_raw", "product", "pageType", "status")

val categoriesRaw = spark.read.option("header", "false").csv("/input/categories/categories.csv")
  .toDF("cat_id", "dept_id", "cat_name")

// NEW: Load Products and Order Items
val productsRaw = spark.read.option("header", "false").csv("/input/products/products.csv")
  .toDF("product_id", "cat_id", "product_name", "desc", "price", "image")

val orderItemsRaw = spark.read.option("header", "false").csv("/input/order_items/full_order_items.csv")
  .toDF("order_item_id", "order_id", "product_id", "qty", "subtotal", "price")

// 2. Clean the Weblog Data (Remove URL fragments like /product/...)
val weblogClean = weblogRaw.withColumn("category",
  when(col("category_raw").contains("/product"), substring_index(col("category_raw"), "/product", 1))
    .when(col("category_raw").contains("/add_to_cart"), substring_index(col("category_raw"), "/add_to_cart", 1))
    .otherwise(col("category_raw"))
)

// 3. Register Temporary SQL Views so our query can find them
weblogClean.createOrReplaceTempView("weblog")
categoriesRaw.createOrReplaceTempView("categories")
productsRaw.createOrReplaceTempView("products")
orderItemsRaw.createOrReplaceTempView("order_items")

// 4. Execute your EXACT updated SQL query
val sqlQuery = """
  SELECT
      COALESCE(w.category_name, s.category_name) AS category,
      CASE
          WHEN w.category_name IS NOT NULL AND s.category_name IS NOT NULL THEN 'Both (Web & Store)'
          WHEN w.category_name IS NOT NULL THEN 'Web Only'
          ELSE 'Store Only'
      END AS presence
  FROM (
      SELECT DISTINCT LOWER(TRIM(category)) AS category_name
      FROM weblog
      WHERE pageType IN ('add_to_cart', 'product', 'category')
        AND category IS NOT NULL
        AND category != ''
  ) w
  FULL OUTER JOIN (
      SELECT DISTINCT LOWER(TRIM(c.cat_name)) AS category_name
      FROM order_items oi
      JOIN products p  ON oi.product_id = p.product_id
      JOIN categories c ON p.cat_id = c.cat_id
  ) s
  ON w.category_name = s.category_name
  ORDER BY presence, category
"""

val rawResult = spark.sql(sqlQuery)

// 5. Format Output (Capitalize the category names)
val finalResult = rawResult.withColumn("Category", initcap(col("category"))).select("Category", "presence")

// 6. Save directly back to HDFS as a single CSV
finalResult.coalesce(1).write.option("header", "true").mode("overwrite").csv("/output/spark_analysis_v2")

println("SUCCESS: Updated Spark SQL Analysis Complete!")
sys.exit(0)