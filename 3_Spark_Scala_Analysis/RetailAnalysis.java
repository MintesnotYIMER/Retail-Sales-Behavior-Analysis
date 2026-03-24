package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RetailAnalysis {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Retail Web vs Instore Analysis")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")  // ← add this line
                .getOrCreate();

        String hdfs = "hdfs://namenode:9000/user/root/data/";
        String output = "hdfs://namenode:9000/user/root/results/";

        System.out.println("\n=== Loading datasets ===");

        Dataset<Row> weblog = spark.read()
                .option("header", "false")
                .csv(hdfs + "weblog.csv")
                .toDF("ip", "datetime", "dept", "category", "product", "pageType", "respCode");

        Dataset<Row> departments = spark.read()
                .option("header", "false")
                .csv(hdfs + "departments.csv")
                .toDF("dept_id", "dept_name");

        Dataset<Row> categories = spark.read()
                .option("header", "false")
                .csv(hdfs + "categories.csv")
                .toDF("cat_id", "dept_id", "cat_name");

        Dataset<Row> products = spark.read()
                .option("header", "false")
                .csv(hdfs + "products.csv")
                .toDF("product_id", "cat_id", "product_name", "product_desc", "price", "product_url");

        Dataset<Row> customers = spark.read()
                .option("header", "false")
                .csv(hdfs + "customers.csv")
                .toDF("customer_id", "fname", "lname", "email", "password", "street", "city", "state",
                        "zipcode");

        Dataset<Row> orders = spark.read()
                .option("header", "false")
                .csv(hdfs + "full_orders.csv")
                .toDF("order_id", "order_date", "customer_id", "order_status");

        Dataset<Row> orderItems = spark.read()
                .option("header", "false")
                .csv(hdfs + "full_order_items.csv")
                .toDF("item_id", "order_id", "product_id", "quantity", "subtotal", "unit_price");

        weblog.createOrReplaceTempView("weblog");
        departments.createOrReplaceTempView("departments");
        categories.createOrReplaceTempView("categories");
        products.createOrReplaceTempView("products");
        customers.createOrReplaceTempView("customers");
        orders.createOrReplaceTempView("orders");
        orderItems.createOrReplaceTempView("order_items");

        System.out.println("All datasets loaded!\n");

        // ══════════════════════════════════════════════════════════
        // ANALYSIS 1a: Category Presence — Web vs Actual Store Sales
        // Shows only categories that have real confirmed sales
        // Output: which categories are on web only, store only, or both
        // ══════════════════════════════════════════════════════════
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  ANALYSIS 1a: Web vs Actual Store Sales         ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        Dataset<Row> analysis1a = spark.sql(
                "SELECT " +
                        "  COALESCE(w.category_name, s.category_name) AS category, " +
                        "  CASE " +
                        "    WHEN w.category_name IS NOT NULL AND s.category_name IS NOT NULL THEN 'Both (Web & Store)' "
                        +
                        "    WHEN w.category_name IS NOT NULL THEN 'Web Only' " +
                        "    ELSE 'Store Only' " +
                        "  END AS presence " +
                        "FROM ( " +
                        "  SELECT DISTINCT LOWER(TRIM(category)) AS category_name " +
                        "  FROM weblog " +
                        "  WHERE pageType IN ('add_to_cart','product','category') " +
                        "    AND category IS NOT NULL AND category != '' " +
                        ") w " +
                        "FULL OUTER JOIN ( " +
                        "  SELECT DISTINCT LOWER(TRIM(c.cat_name)) AS category_name " +
                        "  FROM order_items oi " +
                        "  JOIN products p   ON oi.product_id = p.product_id " +
                        "  JOIN categories c ON p.cat_id = c.cat_id " +
                        ") s ON w.category_name = s.category_name " +
                        "ORDER BY presence, category");
        analysis1a.show(50, false);

        // Summary: count how many categories fall in each presence group
        System.out.println("-- Analysis 1a Summary: Category presence counts --");
        Dataset<Row> summary1a = spark.sql(
                "WITH presence AS ( " +
                        "  SELECT " +
                        "    CASE " +
                        "      WHEN w.category_name IS NOT NULL AND s.category_name IS NOT NULL THEN 'Both (Web & Store)' "
                        +
                        "      WHEN w.category_name IS NOT NULL THEN 'Web Only' " +
                        "      ELSE 'Store Only' " +
                        "    END AS presence " +
                        "  FROM ( " +
                        "    SELECT DISTINCT LOWER(TRIM(category)) AS category_name " +
                        "    FROM weblog " +
                        "    WHERE pageType IN ('add_to_cart','product','category') " +
                        "      AND category IS NOT NULL AND category != '' " +
                        "  ) w " +
                        "  FULL OUTER JOIN ( " +
                        "    SELECT DISTINCT LOWER(TRIM(c.cat_name)) AS category_name " +
                        "    FROM order_items oi " +
                        "    JOIN products p   ON oi.product_id = p.product_id " +
                        "    JOIN categories c ON p.cat_id = c.cat_id " +
                        "  ) s ON w.category_name = s.category_name " +
                        ") " +
                        "SELECT presence, COUNT(*) AS category_count, " +
                        "  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total " +
                        "FROM presence " +
                        "GROUP BY presence " +
                        "ORDER BY category_count DESC");
        summary1a.show(false);

        analysis1a.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis1a_web_vs_store_sales");
        summary1a.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis1a_summary");

        // ══════════════════════════════════════════════════════════
        // ANALYSIS 1b: Category Presence — Web vs Full Catalog
        // Uses all categories in the DB regardless of whether they were sold
        // Output: what % of the catalog actually has web traffic
        // ══════════════════════════════════════════════════════════
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  ANALYSIS 1b: Web vs Full Catalog               ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        Dataset<Row> analysis1b = spark.sql(
                "SELECT " +
                        "  COALESCE(w.category_name, s.category_name) AS category, " +
                        "  CASE " +
                        "    WHEN w.category_name IS NOT NULL AND s.category_name IS NOT NULL THEN 'Both (Web & Catalog)' "
                        +
                        "    WHEN w.category_name IS NOT NULL THEN 'Web Only' " +
                        "    ELSE 'Catalog Only (no web traffic)' " +
                        "  END AS presence " +
                        "FROM ( " +
                        "  SELECT DISTINCT LOWER(TRIM(category)) AS category_name " +
                        "  FROM weblog " +
                        "  WHERE pageType IN ('add_to_cart','product','category') " +
                        "    AND category IS NOT NULL AND category != '' " +
                        ") w " +
                        "FULL OUTER JOIN ( " +
                        "  SELECT DISTINCT LOWER(TRIM(cat_name)) AS category_name " +
                        "  FROM categories " +
                        ") s ON w.category_name = s.category_name " +
                        "ORDER BY presence, category");
        analysis1b.show(50, false);

        // Summary: single-row catalog coverage overview
        System.out.println("-- Analysis 1b Summary: Catalog coverage overview --");
        Dataset<Row> summary1b = spark.sql(
                "WITH presence AS ( " +
                        "  SELECT " +
                        "    CASE " +
                        "      WHEN w.category_name IS NOT NULL AND s.category_name IS NOT NULL THEN 'Both' "
                        +
                        "      WHEN w.category_name IS NOT NULL THEN 'Web Only' " +
                        "      ELSE 'Catalog Only' " +
                        "    END AS presence " +
                        "  FROM ( " +
                        "    SELECT DISTINCT LOWER(TRIM(category)) AS category_name " +
                        "    FROM weblog " +
                        "    WHERE pageType IN ('add_to_cart','product','category') " +
                        "      AND category IS NOT NULL AND category != '' " +
                        "  ) w " +
                        "  FULL OUTER JOIN ( " +
                        "    SELECT DISTINCT LOWER(TRIM(cat_name)) AS category_name " +
                        "    FROM categories " +
                        "  ) s ON w.category_name = s.category_name " +
                        "), " +
                        "counts AS ( " +
                        "  SELECT " +
                        "    SUM(CASE WHEN presence = 'Both'         THEN 1 ELSE 0 END) AS both, "
                        +
                        "    SUM(CASE WHEN presence = 'Web Only'     THEN 1 ELSE 0 END) AS web_only, "
                        +
                        "    SUM(CASE WHEN presence = 'Catalog Only' THEN 1 ELSE 0 END) AS catalog_only "
                        +
                        "  FROM presence " +
                        ") " +
                        "SELECT both, web_only, catalog_only, " +
                        "  both + web_only                                          AS total_web_categories, "
                        +
                        "  both + catalog_only                                      AS total_catalog_categories, "
                        +
                        "  ROUND(both * 100.0 / NULLIF(both + catalog_only, 0), 2) AS catalog_coverage_pct "
                        +
                        "FROM counts");
        summary1b.show(false);

        analysis1b.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis1b_web_vs_catalog");
        summary1b.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis1b_summary");

        // ══════════════════════════════════════════════════════════
        // ANALYSIS 2: Sales Proportions (%) per Category
        // Compares the percentage share each category holds on web vs store
        // Output: large pct_difference = category is over/under represented on one
        // channel
        // ══════════════════════════════════════════════════════════
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  ANALYSIS 2: Category Sales Proportions (%)     ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        Dataset<Row> analysis2 = spark.sql(
                "WITH web AS ( " +
                        "  SELECT LOWER(TRIM(category)) AS category_name, COUNT(*) AS cnt, " +
                        "         SUM(COUNT(*)) OVER() AS total " +
                        "  FROM weblog " +
                        "  WHERE pageType IN ('add_to_cart','product','category') " +
                        "    AND category IS NOT NULL AND category != '' " +
                        "  GROUP BY LOWER(TRIM(category)) " +
                        "), " +
                        "store AS ( " +
                        "  SELECT LOWER(TRIM(c.cat_name)) AS category_name, COUNT(*) AS cnt, " +
                        "         SUM(COUNT(*)) OVER() AS total " +
                        "  FROM order_items oi " +
                        "  JOIN products p   ON oi.product_id = p.product_id " +
                        "  JOIN categories c ON p.cat_id = c.cat_id " +
                        "  GROUP BY LOWER(TRIM(c.cat_name)) " +
                        ") " +
                        "SELECT COALESCE(w.category_name, s.category_name) AS category, " +
                        "       COALESCE(w.cnt, 0)                              AS web_events, "
                        +
                        "       ROUND(COALESCE(w.cnt * 100.0 / w.total, 0), 2) AS web_pct, " +
                        "       COALESCE(s.cnt, 0)                              AS store_sales, "
                        +
                        "       ROUND(COALESCE(s.cnt * 100.0 / s.total, 0), 2) AS store_pct, " +
                        "       ROUND(ABS(COALESCE(w.cnt * 100.0 / w.total, 0) " +
                        "               - COALESCE(s.cnt * 100.0 / s.total, 0)), 2) AS pct_difference "
                        +
                        "FROM web w " +
                        "FULL OUTER JOIN store s ON w.category_name = s.category_name " +
                        "ORDER BY (COALESCE(w.cnt,0) + COALESCE(s.cnt,0)) DESC");
        analysis2.show(50, false);

        // Summary: top 5 most imbalanced categories between web and store
        System.out.println("-- Analysis 2 Summary: Top 5 most imbalanced categories --");
        spark.sql(
                        "WITH web AS ( " +
                                "  SELECT LOWER(TRIM(category)) AS category_name, COUNT(*) AS cnt, " +
                                "         SUM(COUNT(*)) OVER() AS total " +
                                "  FROM weblog " +
                                "  WHERE pageType IN ('add_to_cart','product','category') " +
                                "    AND category IS NOT NULL AND category != '' " +
                                "  GROUP BY LOWER(TRIM(category)) " +
                                "), " +
                                "store AS ( " +
                                "  SELECT LOWER(TRIM(c.cat_name)) AS category_name, COUNT(*) AS cnt, " +
                                "         SUM(COUNT(*)) OVER() AS total " +
                                "  FROM order_items oi " +
                                "  JOIN products p   ON oi.product_id = p.product_id " +
                                "  JOIN categories c ON p.cat_id = c.cat_id " +
                                "  GROUP BY LOWER(TRIM(c.cat_name)) " +
                                ") " +
                                "SELECT COALESCE(w.category_name, s.category_name) AS category, " +
                                "       ROUND(COALESCE(w.cnt * 100.0 / w.total, 0), 2) AS web_pct, " +
                                "       ROUND(COALESCE(s.cnt * 100.0 / s.total, 0), 2) AS store_pct, " +
                                "       ROUND(ABS(COALESCE(w.cnt * 100.0 / w.total, 0) " +
                                "               - COALESCE(s.cnt * 100.0 / s.total, 0)), 2) AS pct_difference "
                                +
                                "FROM web w FULL OUTER JOIN store s ON w.category_name = s.category_name "
                                +
                                "ORDER BY pct_difference DESC " +
                                "LIMIT 5")
                .show(false);

        analysis2.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis2_sales_proportions");

        // ══════════════════════════════════════════════════════════
        // ANALYSIS 3a: Top Products — Web Rank vs Store Rank
        // Ranks products independently on each channel
        // Output: rank_gap reveals products popular online but not selling (or vice
        // versa)
        // ══════════════════════════════════════════════════════════
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  ANALYSIS 3a: Product Rank Web vs Store         ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        Dataset<Row> analysis3a = spark.sql(
                "WITH web_ranked AS ( " +
                        "  SELECT LOWER(TRIM(product)) AS product_name, " +
                        "         COUNT(*) AS web_cart_adds, " +
                        "         RANK() OVER (ORDER BY COUNT(*) DESC) AS web_rank " +
                        "  FROM weblog " +
                        "  WHERE pageType = 'add_to_cart' " +
                        "    AND product IS NOT NULL AND product != '' " +
                        "  GROUP BY LOWER(TRIM(product)) " +
                        "), " +
                        "store_ranked AS ( " +
                        "  SELECT LOWER(TRIM(p.product_name)) AS product_name, " +
                        "         SUM(CAST(oi.quantity AS INT)) AS store_qty_sold, " +
                        "         RANK() OVER (ORDER BY SUM(CAST(oi.quantity AS INT)) DESC) AS store_rank "
                        +
                        "  FROM order_items oi " +
                        "  JOIN products p ON oi.product_id = p.product_id " +
                        "  GROUP BY LOWER(TRIM(p.product_name)) " +
                        ") " +
                        "SELECT COALESCE(w.product_name, s.product_name) AS product, " +
                        "       COALESCE(w.web_cart_adds, 0)  AS web_cart_adds, " +
                        "       COALESCE(w.web_rank, 9999)    AS web_rank, " +
                        "       COALESCE(s.store_qty_sold, 0) AS store_qty_sold, " +
                        "       COALESCE(s.store_rank, 9999)  AS store_rank, " +
                        "       (COALESCE(s.store_rank, 9999) - COALESCE(w.web_rank, 9999)) AS rank_gap "
                        +
                        "FROM web_ranked w " +
                        "FULL OUTER JOIN store_ranked s ON w.product_name = s.product_name " +
                        "ORDER BY COALESCE(w.web_cart_adds, 0) DESC");
        analysis3a.show(20, false);

        // Summary: biggest rank mismatches (products popular on web but not in store)
        System.out.println("-- Analysis 3a Summary: Top 5 biggest rank gaps (web popular, store low) --");
        spark.sql(
                        "WITH web_ranked AS ( " +
                                "  SELECT LOWER(TRIM(product)) AS product_name, COUNT(*) AS web_cart_adds, "
                                +
                                "         RANK() OVER (ORDER BY COUNT(*) DESC) AS web_rank " +
                                "  FROM weblog WHERE pageType = 'add_to_cart' " +
                                "    AND product IS NOT NULL AND product != '' " +
                                "  GROUP BY LOWER(TRIM(product)) " +
                                "), " +
                                "store_ranked AS ( " +
                                "  SELECT LOWER(TRIM(p.product_name)) AS product_name, " +
                                "         SUM(CAST(oi.quantity AS INT)) AS store_qty_sold, " +
                                "         RANK() OVER (ORDER BY SUM(CAST(oi.quantity AS INT)) DESC) AS store_rank "
                                +
                                "  FROM order_items oi JOIN products p ON oi.product_id = p.product_id "
                                +
                                "  GROUP BY LOWER(TRIM(p.product_name)) " +
                                ") " +
                                "SELECT COALESCE(w.product_name, s.product_name) AS product, " +
                                "       COALESCE(w.web_rank, 9999) AS web_rank, " +
                                "       COALESCE(s.store_rank, 9999) AS store_rank, " +
                                "       ABS(COALESCE(s.store_rank,9999) - COALESCE(w.web_rank,9999)) AS abs_gap "
                                +
                                "FROM web_ranked w FULL OUTER JOIN store_ranked s ON w.product_name = s.product_name "
                                +
                                "WHERE w.product_name IS NOT NULL " +
                                "ORDER BY abs_gap DESC LIMIT 5")
                .show(false);

        analysis3a.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis3a_product_rank");

        // ══════════════════════════════════════════════════════════
        // ANALYSIS 3b: Product Presence — Web vs Store
        // Full product comparison with cart adds and quantity sold
        // Output: presence flag + conversion rate per product
        // ══════════════════════════════════════════════════════════
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  ANALYSIS 3b: Product Presence Web vs Store     ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        Dataset<Row> analysis3b = spark.sql(
                "WITH web_sales AS ( " +
                        "  SELECT LOWER(TRIM(product)) AS product_name, COUNT(*) AS web_cart_adds "
                        +
                        "  FROM weblog " +
                        "  WHERE pageType = 'add_to_cart' AND product IS NOT NULL AND product != '' "
                        +
                        "  GROUP BY LOWER(TRIM(product)) " +
                        "), " +
                        "actual_sales AS ( " +
                        "  SELECT LOWER(TRIM(p.product_name)) AS product_name, " +
                        "         COUNT(oi.item_id)                        AS order_line_count, "
                        +
                        "         SUM(CAST(oi.quantity AS INT))            AS total_qty_sold, "
                        +
                        "         ROUND(SUM(CAST(oi.subtotal AS DOUBLE)),2) AS total_revenue, "
                        +
                        "         ROUND(AVG(CAST(oi.quantity AS INT)),2)   AS avg_qty_per_order "
                        +
                        "  FROM order_items oi " +
                        "  JOIN products p ON oi.product_id = p.product_id " +
                        "  GROUP BY LOWER(TRIM(p.product_name)) " +
                        ") " +
                        "SELECT COALESCE(w.product_name, s.product_name) AS product, " +
                        "       COALESCE(w.web_cart_adds, 0)   AS web_cart_adds, " +
                        "       COALESCE(s.order_line_count, 0) AS actual_order_lines, " +
                        "       COALESCE(s.total_qty_sold, 0)   AS total_qty_sold, " +
                        "       COALESCE(s.avg_qty_per_order,0) AS avg_qty_per_order, " +
                        "       COALESCE(s.total_revenue, 0)    AS total_revenue, " +
                        "       ROUND(COALESCE(w.web_cart_adds,0) * 1.0 " +
                        "           / NULLIF(s.total_qty_sold, 0), 2)      AS web_events_per_unit_sold, "
                        +
                        "       ROUND(COALESCE(s.order_line_count,0) * 100.0 " +
                        "           / NULLIF(w.web_cart_adds, 0), 2)       AS cart_to_order_pct, "
                        +
                        "       CASE " +
                        "         WHEN w.product_name IS NOT NULL AND s.product_name IS NOT NULL THEN 'Both (Web & Store)' "
                        +
                        "         WHEN w.product_name IS NOT NULL THEN 'Web Only (abandoned)' "
                        +
                        "         ELSE 'Store Only' " +
                        "       END AS presence " +
                        "FROM web_sales w " +
                        "FULL OUTER JOIN actual_sales s ON w.product_name = s.product_name " +
                        "ORDER BY total_revenue DESC");
        analysis3b.show(20, false);

        // Summary: presence counts + total revenue per group
        System.out.println("-- Analysis 3b Summary: Revenue and count per presence type --");
        spark.sql(
                        "WITH web_sales AS ( " +
                                "  SELECT LOWER(TRIM(product)) AS product_name, COUNT(*) AS web_cart_adds "
                                +
                                "  FROM weblog WHERE pageType = 'add_to_cart' " +
                                "    AND product IS NOT NULL AND product != '' " +
                                "  GROUP BY LOWER(TRIM(product)) " +
                                "), " +
                                "actual_sales AS ( " +
                                "  SELECT LOWER(TRIM(p.product_name)) AS product_name, " +
                                "         SUM(CAST(oi.subtotal AS DOUBLE)) AS total_revenue " +
                                "  FROM order_items oi JOIN products p ON oi.product_id = p.product_id "
                                +
                                "  GROUP BY LOWER(TRIM(p.product_name)) " +
                                "), " +
                                "flagged AS ( " +
                                "  SELECT " +
                                "    CASE " +
                                "      WHEN w.product_name IS NOT NULL AND s.product_name IS NOT NULL THEN 'Both (Web & Store)' "
                                +
                                "      WHEN w.product_name IS NOT NULL THEN 'Web Only (abandoned)' " +
                                "      ELSE 'Store Only' " +
                                "    END AS presence, " +
                                "    COALESCE(s.total_revenue, 0) AS total_revenue " +
                                "  FROM web_sales w FULL OUTER JOIN actual_sales s ON w.product_name = s.product_name "
                                +
                                ") " +
                                "SELECT presence, " +
                                "       COUNT(*)                              AS product_count, " +
                                "       ROUND(SUM(total_revenue), 2)         AS total_revenue, " +
                                "       ROUND(AVG(total_revenue), 2)         AS avg_revenue_per_product "
                                +
                                "FROM flagged " +
                                "GROUP BY presence " +
                                "ORDER BY total_revenue DESC")
                .show(false);

        analysis3b.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis3b_product_presence");

        // ══════════════════════════════════════════════════════════
        // ANALYSIS 4: Department Traffic vs Revenue
        // Compares web traffic share vs revenue share per department
        // Output: revenue_vs_traffic_gap flags departments with conversion problems
        // ══════════════════════════════════════════════════════════
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  ANALYSIS 4: Department Traffic vs Revenue      ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        Dataset<Row> analysis4 = spark.sql(
                "WITH web_dept AS ( " +
                        "  SELECT LOWER(TRIM(dept)) AS dept_name, " +
                        "         COUNT(*) AS web_total_events, " +
                        "         COUNT(CASE WHEN pageType = 'add_to_cart' THEN 1 END) AS web_cart_adds, "
                        +
                        "         SUM(COUNT(*)) OVER() AS grand_total_events " +
                        "  FROM weblog WHERE dept IS NOT NULL AND dept != '' " +
                        "  GROUP BY LOWER(TRIM(dept)) " +
                        "), " +
                        "store_dept AS ( " +
                        "  SELECT LOWER(TRIM(d.dept_name)) AS dept_name, " +
                        "         SUM(CAST(oi.quantity AS INT))           AS total_qty_sold, " +
                        "         ROUND(SUM(CAST(oi.subtotal AS DOUBLE)), 2) AS total_revenue, "
                        +
                        "         SUM(SUM(CAST(oi.subtotal AS DOUBLE))) OVER() AS grand_total_revenue "
                        +
                        "  FROM order_items oi " +
                        "  JOIN products p    ON oi.product_id = p.product_id " +
                        "  JOIN categories c  ON p.cat_id = c.cat_id " +
                        "  JOIN departments d ON c.dept_id = d.dept_id " +
                        "  GROUP BY LOWER(TRIM(d.dept_name)) " +
                        ") " +
                        "SELECT COALESCE(w.dept_name, s.dept_name)             AS department, "
                        +
                        "       COALESCE(w.web_total_events, 0)                AS web_total_events, "
                        +
                        "       COALESCE(w.web_cart_adds, 0)                   AS web_cart_adds, "
                        +
                        "       ROUND(COALESCE(w.web_total_events * 100.0 / w.grand_total_events, 0), 2) AS web_traffic_pct, "
                        +
                        "       COALESCE(s.total_qty_sold, 0)                  AS store_qty_sold, "
                        +
                        "       COALESCE(s.total_revenue, 0)                   AS store_revenue, "
                        +
                        "       ROUND(COALESCE(s.total_revenue * 100.0 / s.grand_total_revenue, 0), 2)   AS revenue_pct, "
                        +
                        "       ROUND(COALESCE(s.total_revenue * 100.0 / s.grand_total_revenue, 0) "
                        +
                        "           - COALESCE(w.web_total_events * 100.0 / w.grand_total_events, 0), 2) "
                        +
                        "         AS revenue_vs_traffic_gap " +
                        "FROM web_dept w " +
                        "FULL OUTER JOIN store_dept s ON w.dept_name = s.dept_name " +
                        "ORDER BY store_revenue DESC");
        analysis4.show(false);

        // Summary: flag departments as efficient or underperforming based on gap
        System.out.println("-- Analysis 4 Summary: Department efficiency classification --");
        spark.sql(
                        "WITH web_dept AS ( " +
                                "  SELECT LOWER(TRIM(dept)) AS dept_name, COUNT(*) AS web_total_events, "
                                +
                                "         SUM(COUNT(*)) OVER() AS grand_total_events " +
                                "  FROM weblog WHERE dept IS NOT NULL AND dept != '' " +
                                "  GROUP BY LOWER(TRIM(dept)) " +
                                "), " +
                                "store_dept AS ( " +
                                "  SELECT LOWER(TRIM(d.dept_name)) AS dept_name, " +
                                "         ROUND(SUM(CAST(oi.subtotal AS DOUBLE)), 2) AS total_revenue, "
                                +
                                "         SUM(SUM(CAST(oi.subtotal AS DOUBLE))) OVER() AS grand_total_revenue "
                                +
                                "  FROM order_items oi " +
                                "  JOIN products p ON oi.product_id = p.product_id " +
                                "  JOIN categories c ON p.cat_id = c.cat_id " +
                                "  JOIN departments d ON c.dept_id = d.dept_id " +
                                "  GROUP BY LOWER(TRIM(d.dept_name)) " +
                                ") " +
                                "SELECT COALESCE(w.dept_name, s.dept_name) AS department, " +
                                "       ROUND(COALESCE(s.total_revenue * 100.0 / s.grand_total_revenue, 0) "
                                +
                                "           - COALESCE(w.web_total_events * 100.0 / w.grand_total_events, 0), 2) AS gap, "
                                +
                                "       CASE " +
                                "         WHEN (COALESCE(s.total_revenue * 100.0 / s.grand_total_revenue, 0) "
                                +
                                "             - COALESCE(w.web_total_events * 100.0 / w.grand_total_events, 0)) > 2 "
                                +
                                "           THEN 'Efficient (high revenue, low web need)' " +
                                "         WHEN (COALESCE(s.total_revenue * 100.0 / s.grand_total_revenue, 0) "
                                +
                                "             - COALESCE(w.web_total_events * 100.0 / w.grand_total_events, 0)) < -2 "
                                +
                                "           THEN 'Underperforming (high traffic, low revenue)' " +
                                "         ELSE 'Balanced' " +
                                "       END AS classification " +
                                "FROM web_dept w FULL OUTER JOIN store_dept s ON w.dept_name = s.dept_name "
                                +
                                "ORDER BY gap DESC")
                .show(false);

        analysis4.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis4_dept_traffic_vs_revenue");

        // ══════════════════════════════════════════════════════════
        // ANALYSIS 5: Web Sales Funnel per Category
        // Tracks product_view → view_cart → checkout → add_to_cart
        // Output: drop-off rates show where customers abandon the journey
        // ══════════════════════════════════════════════════════════
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  ANALYSIS 5: Web Sales Funnel per Category      ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        Dataset<Row> analysis5 = spark.sql(
                "WITH funnel AS ( " +
                        "  SELECT LOWER(TRIM(category)) AS category, pageType " +
                        "  FROM weblog " +
                        "  WHERE pageType IN ('product','view_cart','checkout','add_to_cart') "
                        +
                        "    AND respCode = '200' " +
                        "    AND category IS NOT NULL AND category != '' " +
                        ") " +
                        "SELECT category, " +
                        "  COUNT(CASE WHEN pageType='product'     THEN 1 END) AS product_views, "
                        +
                        "  COUNT(CASE WHEN pageType='view_cart'   THEN 1 END) AS cart_views, " +
                        "  COUNT(CASE WHEN pageType='checkout'    THEN 1 END) AS checkouts, " +
                        "  COUNT(CASE WHEN pageType='add_to_cart' THEN 1 END) AS add_to_carts, "
                        +
                        "  ROUND(COUNT(CASE WHEN pageType='view_cart' THEN 1 END) * 100.0 " +
                        "      / NULLIF(COUNT(CASE WHEN pageType='product' THEN 1 END),0), 2) AS pct_view_to_cart, "
                        +
                        "  ROUND(COUNT(CASE WHEN pageType='checkout' THEN 1 END) * 100.0 " +
                        "      / NULLIF(COUNT(CASE WHEN pageType='view_cart' THEN 1 END),0), 2) AS pct_cart_to_checkout, "
                        +
                        "  ROUND(COUNT(CASE WHEN pageType='add_to_cart' THEN 1 END) * 100.0 " +
                        "      / NULLIF(COUNT(CASE WHEN pageType='checkout' THEN 1 END),0), 2) AS pct_checkout_to_add "
                        +
                        "FROM funnel " +
                        "GROUP BY category " +
                        "ORDER BY add_to_carts DESC");
        analysis5.show(50, false);

        // Summary: overall funnel numbers + best/worst category
        System.out.println("-- Analysis 5 Summary: Overall funnel totals --");
        spark.sql(
                        "WITH funnel AS ( " +
                                "  SELECT pageType FROM weblog " +
                                "  WHERE pageType IN ('product','view_cart','checkout','add_to_cart') "
                                +
                                "    AND respCode = '200' AND category IS NOT NULL AND category != '' "
                                +
                                ") " +
                                "SELECT " +
                                "  COUNT(CASE WHEN pageType='product'     THEN 1 END) AS total_product_views, "
                                +
                                "  COUNT(CASE WHEN pageType='view_cart'   THEN 1 END) AS total_cart_views, "
                                +
                                "  COUNT(CASE WHEN pageType='checkout'    THEN 1 END) AS total_checkouts, "
                                +
                                "  COUNT(CASE WHEN pageType='add_to_cart' THEN 1 END) AS total_add_to_carts, "
                                +
                                "  ROUND(COUNT(CASE WHEN pageType='view_cart' THEN 1 END) * 100.0 " +
                                "      / NULLIF(COUNT(CASE WHEN pageType='product' THEN 1 END),0), 2) AS overall_view_to_cart_pct, "
                                +
                                "  ROUND(COUNT(CASE WHEN pageType='checkout' THEN 1 END) * 100.0 " +
                                "      / NULLIF(COUNT(CASE WHEN pageType='view_cart' THEN 1 END),0), 2) AS overall_cart_to_checkout_pct, "
                                +
                                "  ROUND(COUNT(CASE WHEN pageType='add_to_cart' THEN 1 END) * 100.0 " +
                                "      / NULLIF(COUNT(CASE WHEN pageType='checkout' THEN 1 END),0), 2) AS overall_checkout_to_add_pct "
                                +
                                "FROM funnel")
                .show(false);

        analysis5.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis5_funnel_per_category");

        // ══════════════════════════════════════════════════════════
        // ANALYSIS 6: Price Influence on Sales
        // Buckets products by price range, compares web adds vs store qty
        // Output: which price segments convert best on each channel
        // ══════════════════════════════════════════════════════════
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  ANALYSIS 6: Price Influence on Sales           ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        Dataset<Row> analysis6 = spark.sql(
                "WITH ranges AS ( " +
                        "  SELECT product_id, product_name, CAST(price AS DOUBLE) AS price, " +
                        "    CASE " +
                        "      WHEN CAST(price AS DOUBLE) < 50  THEN '1. $0-$50' " +
                        "      WHEN CAST(price AS DOUBLE) < 100 THEN '2. $50-$100' " +
                        "      WHEN CAST(price AS DOUBLE) < 200 THEN '3. $100-$200' " +
                        "      ELSE '4. $200+' " +
                        "    END AS price_range " +
                        "  FROM products WHERE price IS NOT NULL " +
                        "), " +
                        "web_agg AS ( " +
                        "  SELECT LOWER(TRIM(product)) AS product_name, COUNT(*) AS web_cart_adds "
                        +
                        "  FROM weblog WHERE pageType = 'add_to_cart' " +
                        "  GROUP BY LOWER(TRIM(product)) " +
                        "), " +
                        "store_agg AS ( " +
                        "  SELECT product_id, SUM(CAST(quantity AS INT)) AS store_qty_sold " +
                        "  FROM order_items GROUP BY product_id " +
                        ") " +
                        "SELECT r.price_range, " +
                        "       ROUND(AVG(r.price), 2)                     AS avg_price, " +
                        "       COUNT(DISTINCT w.product_name)             AS web_unique_products, "
                        +
                        "       COALESCE(SUM(w.web_cart_adds), 0)         AS web_cart_adds, " +
                        "       COUNT(DISTINCT s.product_id)              AS store_unique_products, "
                        +
                        "       COALESCE(SUM(s.store_qty_sold), 0)        AS store_qty_sold, " +
                        "       ROUND(COALESCE(SUM(w.web_cart_adds),0) * 1.0 " +
                        "           / NULLIF(COALESCE(SUM(s.store_qty_sold),0), 0), 2) AS web_events_per_unit "
                        +
                        "FROM ranges r " +
                        "LEFT JOIN web_agg w   ON LOWER(TRIM(r.product_name)) = w.product_name "
                        +
                        "LEFT JOIN store_agg s ON r.product_id = s.product_id " +
                        "GROUP BY r.price_range " +
                        "ORDER BY r.price_range");
        analysis6.show(false);

        // Summary: which price range has best conversion (lowest web_events_per_unit)
        System.out.println("-- Analysis 6 Summary: Best converting price range --");
        spark.sql(
                        "WITH ranges AS ( " +
                                "  SELECT product_id, product_name, CAST(price AS DOUBLE) AS price, " +
                                "    CASE WHEN CAST(price AS DOUBLE) < 50  THEN '1. $0-$50' " +
                                "         WHEN CAST(price AS DOUBLE) < 100 THEN '2. $50-$100' " +
                                "         WHEN CAST(price AS DOUBLE) < 200 THEN '3. $100-$200' " +
                                "         ELSE '4. $200+' END AS price_range " +
                                "  FROM products WHERE price IS NOT NULL " +
                                "), " +
                                "web_agg AS ( " +
                                "  SELECT LOWER(TRIM(product)) AS product_name, COUNT(*) AS web_cart_adds "
                                +
                                "  FROM weblog WHERE pageType='add_to_cart' GROUP BY LOWER(TRIM(product)) "
                                +
                                "), " +
                                "store_agg AS ( " +
                                "  SELECT product_id, SUM(CAST(quantity AS INT)) AS store_qty_sold " +
                                "  FROM order_items GROUP BY product_id " +
                                ") " +
                                "SELECT r.price_range, " +
                                "  ROUND(COALESCE(SUM(w.web_cart_adds),0) * 1.0 " +
                                "      / NULLIF(COALESCE(SUM(s.store_qty_sold),0),0), 2) AS web_events_per_unit, "
                                +
                                "  CASE WHEN ROUND(COALESCE(SUM(w.web_cart_adds),0) * 1.0 " +
                                "      / NULLIF(COALESCE(SUM(s.store_qty_sold),0),0), 2) < 1 " +
                                "    THEN 'Best Converter' ELSE 'Review Needed' END AS verdict " +
                                "FROM ranges r " +
                                "LEFT JOIN web_agg w   ON LOWER(TRIM(r.product_name)) = w.product_name "
                                +
                                "LEFT JOIN store_agg s ON r.product_id = s.product_id " +
                                "GROUP BY r.price_range ORDER BY web_events_per_unit")
                .show(false);

        analysis6.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis6_price_influence");

        // ══════════════════════════════════════════════════════════
        // ANALYSIS 7: Peak Hours — Web Traffic vs Store Order Placement
        // Compares hour-by-hour web activity vs actual orders placed
        // Output: order_vs_cart_gap reveals browse vs buy time patterns
        // ══════════════════════════════════════════════════════════
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  ANALYSIS 7: Peak Hours Web vs Store Orders     ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        Dataset<Row> analysis7 = spark.sql(
                "WITH web_hours AS ( " +
                        "  SELECT HOUR(TO_TIMESTAMP(datetime, 'dd/MMM/yyyy:HH:mm:ss Z')) AS hour_of_day, "
                        +
                        "         COUNT(*) AS web_events, " +
                        "         COUNT(CASE WHEN pageType='add_to_cart' THEN 1 END) AS web_cart_adds "
                        +
                        "  FROM weblog WHERE datetime IS NOT NULL " +
                        "  GROUP BY HOUR(TO_TIMESTAMP(datetime, 'dd/MMM/yyyy:HH:mm:ss Z')) " +
                        "), " +
                        "order_hours AS ( " +
                        "  SELECT HOUR(TO_TIMESTAMP(order_date, 'yyyy-MM-dd HH:mm:ss')) AS hour_of_day, "
                        +
                        "         COUNT(*) AS total_orders " +
                        "  FROM orders WHERE order_date IS NOT NULL " +
                        "  GROUP BY HOUR(TO_TIMESTAMP(order_date, 'yyyy-MM-dd HH:mm:ss')) " +
                        ") " +
                        "SELECT COALESCE(w.hour_of_day, o.hour_of_day) AS hour_of_day, " +
                        "       COALESCE(w.web_events, 0)              AS web_events, " +
                        "       COALESCE(w.web_cart_adds, 0)           AS web_cart_adds, " +
                        "       COALESCE(o.total_orders, 0)            AS store_orders, " +
                        "       COALESCE(o.total_orders,0) - COALESCE(w.web_cart_adds,0) AS order_vs_cart_gap "
                        +
                        "FROM web_hours w " +
                        "FULL OUTER JOIN order_hours o ON w.hour_of_day = o.hour_of_day " +
                        "ORDER BY hour_of_day");
        analysis7.show(24, false);

        // Summary: peak web hour, peak order hour, and busiest overall hour
        System.out.println("-- Analysis 7 Summary: Peak activity hours --");
        spark.sql(
                        "WITH web_hours AS ( " +
                                "  SELECT HOUR(TO_TIMESTAMP(datetime, 'dd/MMM/yyyy:HH:mm:ss Z')) AS hour_of_day, "
                                +
                                "         COUNT(*) AS web_events, " +
                                "         COUNT(CASE WHEN pageType='add_to_cart' THEN 1 END) AS web_cart_adds "
                                +
                                "  FROM weblog WHERE datetime IS NOT NULL " +
                                "  GROUP BY HOUR(TO_TIMESTAMP(datetime, 'dd/MMM/yyyy:HH:mm:ss Z')) " +
                                "), " +
                                "order_hours AS ( " +
                                "  SELECT HOUR(TO_TIMESTAMP(order_date, 'yyyy-MM-dd HH:mm:ss')) AS hour_of_day, "
                                +
                                "         COUNT(*) AS total_orders " +
                                "  FROM orders WHERE order_date IS NOT NULL " +
                                "  GROUP BY HOUR(TO_TIMESTAMP(order_date, 'yyyy-MM-dd HH:mm:ss')) " +
                                ") " +
                                "SELECT " +
                                "  (SELECT hour_of_day FROM web_hours ORDER BY web_events DESC LIMIT 1)     AS peak_web_hour, "
                                +
                                "  (SELECT hour_of_day FROM web_hours ORDER BY web_cart_adds DESC LIMIT 1)  AS peak_cart_add_hour, "
                                +
                                "  (SELECT hour_of_day FROM order_hours ORDER BY total_orders DESC LIMIT 1) AS peak_order_hour ")
                .show(false);

        analysis7.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(output + "analysis7_peak_hours");

        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  All analyses complete!                         ║");
        System.out.println("║  Results saved to: /user/root/results/          ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        spark.stop();
    }
}
