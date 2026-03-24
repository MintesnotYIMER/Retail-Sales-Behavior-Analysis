# Sales Behavior Analysis: Bridging Web Logs and Retail Transaction Data

## Project Overview
This repository contains the final project for the **Innovative Data Management (M2 2IS)** course at Université Toulouse 1 Capitole. 

The goal of this project is to compare customer browsing behavior with confirmed sales transactions to understand the differences between in-store purchases and online sales. We built a polyglot data processing pipeline using **Hadoop MapReduce (Java)** for unstructured web log ETL, and **Apache Spark (Scala & Java)** for complex relational analytics on structured retail data.

**Authors:** Ibrahim Elhout, Ahmad Ibrahim, Mintesnot Yimer

---

## Tech Stack
* **Ecosystem:** Apache Hadoop, HDFS, Apache Spark
* **Languages:** Java, Scala, Spark SQL
* **Techniques:** MapReduce, DataFrames, Distributed ETL Processing

---

## Repository Structure & Source Code

### Phase 0: Raw Datasets
* 🗄️ **[Web Logs (Access_log_Short_Version.txt)](0_Raw_Data/Web_Logs/Access_log_Short_Version.txt)**: A truncated sample of the raw, unstructured Apache HTTP access log capturing online browsing behavior. *(Note: The original `access.log.2` file exceeds GitHub's size limits, so a representative sample is provided here to demonstrate the ETL pipeline).*
* 🗄️ **[Retail Database (Cloudera)](0_Raw_Data/Retail_DB)**: The raw `.txt` data dumps from the Cloudera retail sample database, representing the physical store's transactional data (Orders, Customers, Departments, Categories, etc.).

### Phase 1: Data Preparation (ETL)
* 📄 **[LogToCSV.java](1_Data_Preparation/LogToCSV.java)**: A Java MapReduce job that parses raw, unstructured Apache HTTP web logs using Regex, extracts key features (IP, datetime, department, category, product, page type), and structures them into a clean CSV format for downstream analysis.

### Phase 2: MapReduce Analytics
* 📄 **[CategoryPresenceAnalysis.java](2_MapReduce_Analysis/CategoryPresenceAnalysis.java)**: A MapReduce job utilizing MultipleInputs to perform a distributed join between web logs and store orders, identifying which categories are exclusive to the web, exclusive to the store, or present in both.
* 📄 **[ProductCoverageAnalysis.java](2_MapReduce_Analysis/ProductCoverageAnalysis.java)**: A MapReduce job that calculates catalog visibility by identifying which products generate web traffic but zero physical sales, and vice versa.

### Phase 3: Spark & Scala Analytics
* 📄 **[RetailAnalysis.java](3_Spark_Scala_Analysis/RetailAnalysis.java)**: A comprehensive Apache Spark application (written in Java) utilizing Spark SQL and DataFrames. It executes advanced queries such as Web Sales Funnel Progression (conversion rates), Price Tier Impact on conversion friction, and Peak Hour Analysis.
* 📄 **[CategoryPresence.scala](3_Spark_Scala_Analysis/CategoryPresence.scala)**: A functional Scala script utilizing Spark DataFrames to perform the category overlap analysis, demonstrating the contrast in code complexity between Spark SQL/Scala and traditional Java MapReduce.

### Phase 4: Final Report
* 📕 **[Retail Analysis Report.pdf](4_Report_and_Results/Retail%20Analysis%20Report.pdf)**: The comprehensive final report detailing our methodology, technical justifications, 7 distinct sales behavior analyses, and strategic business recommendations based on the data.

### Phase 5: Final Analytics Output
For convenience, the aggregated outputs of our Hadoop and Spark jobs are available in the repository without needing to execute the code. Here are the highlights:

* 📊 **[Price Tier Influence](5_Final_Outputs/analysis6_price_influence.csv)**: Spark analysis revealing how product price ranges dictate web cart additions vs. physical store checkouts.
* 📊 **[Web Sales Funnel](5_Final_Outputs/analysis5_funnel_per_category.csv)**: Conversion rate analysis from product view -> cart -> checkout by category.
* 📊 **[Department Revenue vs. Traffic](5_Final_Outputs/analysis4_dept_traffic_vs_revenue.csv)**: Identifies departments with massive web traffic but low physical sales (e.g., Outdoors & Fitness).
* 📊 **[Product Presence & Revenue](5_Final_Outputs/analysis3b_product_presence.csv)**: Deep dive into individual product profitability, comparing web cart adds to actual store revenue.
* 📊 **[Category Overlap (MapReduce vs Scala)](5_Final_Outputs/)**: Contains the `Resultmapreduce_analysis.csv` and `ResultScala.csv` proving identical logical execution across both processing paradigms.

*(All 10 intermediate and summary reports from Analyses 1 through 7 are available in the `5_Final_Outputs/` directory).*

---

## Key Findings

Our analysis revealed significant discrepancies between online browsing and physical purchasing. For instance, high-ticket items generated massive web traffic but suffered from a 97% cart abandonment rate, indicating high "research" intent online but physical purchasing preference. 

---

## How to Run

### 1. Data Preparation (ETL)
Compile `LogToCSV.java` into a `.jar` and run it against the raw `access.log.2` file on HDFS to generate `weblog.csv`:
```bash
hadoop jar RetailAnalytics.jar org.example.LogToCSV /input/raw_logs/access.log.2 /output/cleaned_logs
