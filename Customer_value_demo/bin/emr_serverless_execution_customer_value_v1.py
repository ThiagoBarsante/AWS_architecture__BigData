import sys
from datetime import datetime
from pyspark.sql import SparkSession

### EMR serverless execution
AWS_EXECUTION = True ## True 

def fnc_validate_parameters(awsExec_v=AWS_EXECUTION):
    if (len(sys.argv) != 4) and awsExec_v:
        print("Usage: spark-etl ['input folder'] ['output folder'] ['rpt_folder']")
        sys.exit(-1)
        
    # if not(awsExec_v): ## local execution
    #     sys.argv = ['python-script.py', '../s3_data/input/', '../s3_data/output/', '../s3_data/rpt/']

    input_location = sys.argv[1]
    output_location = sys.argv[2]
    rpt_location = sys.argv[3]

    return input_location, output_location, rpt_location


(input_location, output_location, rpt_location) = fnc_validate_parameters()


# print('Input location: ', input_location)
# print('Output location: ', output_location)
# print('Rpt location: ', rpt_location)

spark = SparkSession\
    .builder\
    .appName('SparkETL')\
    .enableHiveSupport()\
    .getOrCreate()

def fshape(dataframe1):
    print('Shape : ', dataframe1.count(), len(dataframe1.columns))


dbname = 'DBM' ## database Marketing
tablename = 'TBP_CUSTOMER_CLV_SERVERLESS' ## Parquet table - Customer Lifetime Value
spark_ml_table = 'TB_ML_SPARK_SDF'


def fnc_show_db_tables():
    spark.sql( ' SHOW DATABASES ').show()
    spark.sql(' SHOW TABLES ').show()


def spark_sql_write_glue_database(db_name, table_name, parquet_output_location=output_location, temp_table=spark_ml_table):
    print(' database creation: ', db_name)
    spark.sql(f" CREATE database if not exists {db_name} ")
    print(' table name creation , ', table_name)
    spark.sql((
        f" CREATE TABLE IF NOT EXISTS {db_name}.{table_name} "
        f" USING PARQUET LOCATION '{parquet_output_location}' AS SELECT * FROM {temp_table}"
    ))


## read local file
sdf = spark.read.parquet(input_location)

## ETL
sdf.createOrReplaceTempView('TB_SALES_SDF')
# spark.sql('select max(TO_DATE(InvoiceDate)) as current_date_for_FRMV_CLV, current_date as not_today from TB_SALES_SDF').show()


## formula to calculate CLV 
def fnc_customer_clv_udf(monetary_value_f, frequency_f, recency_f, discount_f=0.1):
    return round ( ( (monetary_value_f / frequency_f) * (1 - ((recency_f + 1) / 365)) / (1 + discount_f) ) , 2)

## Register the formula to be used by Spark-SQL
from pyspark.sql.types import FloatType

spark.udf.register('fnc_customer_clv_udf', fnc_customer_clv_udf, FloatType())

## Apply some filters and create the main customer purchase history as an example
sql_query_clv = """
WITH TB_SALES_V AS
(
    SELECT CustomerID as customer_id
        , COUNT(DISTINCT (InvoiceDate))  as frequency
        , DATEDIFF( current_date , MAX (InvoiceDate) )  as recency_now
        , ROUND(SUM(Quantity * UnitPrice), 2) as monetary_value
        , ROUND(avg(Quantity * UnitPrice), 2) as avg_revenue
        , MIN(InvoiceDate) as dt_first_Invoice
        , MAX(InvoiceDate) as dt_last_Invoice
        -- , ROUND(AVG(Quantity), 2) as avg_items
        -- , ROUND(SUM(Quantity), 2) as total_items
    FROM TB_SALES_SDF
    WHERE 1 = 1
        AND InvoiceDate IS NOT NULL
        AND Quantity > 0
        AND UnitPrice > 0
    GROUP BY customer_id
)
SELECT tb3.*
  , ROUND ( ( (monetary_value / frequency) * (1 - ((recency_dt + 1) / 365)) / (1 + 0.1) ) , 2) AS CLV_SQL -- discount of 0.1
  , fnc_customer_clv_udf(monetary_value,frequency,recency_dt) AS CLV_UDF
FROM (
    SELECT tb1.*
        , CAST( DATEDIFF(tb2.dt_current_date , tb1.dt_last_Invoice ) as float) as recency_dt
    FROM TB_SALES_V as tb1
    CROSS JOIN (SELECT MAX(dt_last_Invoice) AS dt_current_date FROM TB_SALES_V) tb2
    ) tb3
WHERE 1 = 1
  AND monetary_value > 0
  AND frequency > 0
  AND customer_id IS NOT NULL
ORDER BY monetary_value DESC
"""

sdf_clv = spark.sql(sql_query_clv)
sdf_clv.createOrReplaceTempView(spark_ml_table)


def ml_sql_prediction():
    text_sql_ml2 = f"""
    SELECT 
        {spark_ml_table}.*,
		( CASE
		WHEN ( ( ( `frequency`  > 1.0e1 AND `frequency`  <= 1.14e2 ) ) ) THEN 9
		WHEN ( ((abs(year(`dt_first_Invoice`) - 2.01e3) <= 10e-9) OR ( (`dt_first_Invoice` IS NULL ) ) ) AND ((abs(`frequency` - 1.0e0) <= 10e-9) OR (abs(`frequency` - 2.0e0) <= 10e-9) OR (abs(`frequency` - 3.0e0) <= 10e-9) OR (abs(`frequency` - 4.0e0) <= 10e-9) OR (abs(`frequency` - 5.0e0) <= 10e-9) OR (abs(`frequency` - 6.0e0) <= 10e-9) OR ( (`frequency` IS NULL ) ) ) AND ( ( (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  >= 4.0e0 AND (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  <= 3.4e2 ) OR ( (`dt_last_Invoice` IS NULL ) ) ) ) THEN 1
		WHEN ( ((abs(`frequency` - 7.0e0) <= 10e-9) OR  ( `frequency`  >= 8.0e0 AND `frequency`  <= 1.3e1 ) ) ) THEN 3
		WHEN ( ( ( `recency_dt`  >= 0.0e0 AND `recency_dt`  <= 4.0e0 ) ) ) THEN 10
		WHEN ( ( ( `CLV_SQL`  > 9.0245000000000005e2 AND `CLV_SQL`  <= 7.49729e3 ) ) ) THEN 6
		WHEN ( ( ( `CLV_SQL`  > 3.8501999999999998e2 AND `CLV_SQL`  <= 9.0245000000000005e2 ) ) AND ((abs(`frequency` - 1.0e0) <= 10e-9) OR (abs(`frequency` - 2.0e0) <= 10e-9) OR (abs(`frequency` - 3.0e0) <= 10e-9) OR (abs(`frequency` - 4.0e0) <= 10e-9) OR (abs(`frequency` - 5.0e0) <= 10e-9) OR (abs(`frequency` - 6.0e0) <= 10e-9) OR ( (`frequency` IS NULL ) ) ) ) THEN 2
		WHEN ( ( ( (datediff(concat(year(`dt_first_Invoice`),'-',month(`dt_first_Invoice`),'-',day(`dt_first_Invoice`)),concat(year(`dt_first_Invoice`),'-01-01')) + 1)  > 1.3e1 AND (datediff(concat(year(`dt_first_Invoice`),'-',month(`dt_first_Invoice`),'-',day(`dt_first_Invoice`)),concat(year(`dt_first_Invoice`),'-01-01')) + 1)  <= 6.7e1 ) OR  ( (datediff(concat(year(`dt_first_Invoice`),'-',month(`dt_first_Invoice`),'-',day(`dt_first_Invoice`)),concat(year(`dt_first_Invoice`),'-01-01')) + 1)  > 9.7e1 AND (datediff(concat(year(`dt_first_Invoice`),'-',month(`dt_first_Invoice`),'-',day(`dt_first_Invoice`)),concat(year(`dt_first_Invoice`),'-01-01')) + 1)  <= 1.61e2 ) OR  ( (datediff(concat(year(`dt_first_Invoice`),'-',month(`dt_first_Invoice`),'-',day(`dt_first_Invoice`)),concat(year(`dt_first_Invoice`),'-01-01')) + 1)  > 3.31e2 AND (datediff(concat(year(`dt_first_Invoice`),'-',month(`dt_first_Invoice`),'-',day(`dt_first_Invoice`)),concat(year(`dt_first_Invoice`),'-01-01')) + 1)  <= 3.37e2 ) ) AND ((abs(day(`dt_first_Invoice`) - 8.0e0) <= 10e-9) OR (abs(day(`dt_first_Invoice`) - 1.0e1) <= 10e-9) OR  ( day(`dt_first_Invoice`)  >= 1.1e1 AND day(`dt_first_Invoice`)  <= 1.2e1 ) OR (abs(day(`dt_first_Invoice`) - 1.3e1) <= 10e-9) OR (abs(day(`dt_first_Invoice`) - 1.4e1) <= 10e-9) OR (abs(day(`dt_first_Invoice`) - 1.5e1) <= 10e-9) OR (abs(day(`dt_first_Invoice`) - 1.6e1) <= 10e-9) OR  ( day(`dt_first_Invoice`)  >= 1.9e1 AND day(`dt_first_Invoice`)  <= 2.0e1 ) OR  ( day(`dt_first_Invoice`)  >= 2.1e1 AND day(`dt_first_Invoice`)  <= 2.5e1 ) OR  ( day(`dt_first_Invoice`)  >= 2.6e1 AND day(`dt_first_Invoice`)  <= 3.1e1 ) OR ( (`dt_first_Invoice` IS NULL ) ) ) AND ( ( `recency_dt`  >= 1.0e1 AND `recency_dt`  <= 1.2e1 ) OR  ( `recency_dt`  > 6.4e1 AND `recency_dt`  <= 3.25e2 ) OR ( (`recency_dt` IS NULL ) ) ) AND ((abs(month(`dt_last_Invoice`) - 2.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 5.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 6.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 7.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 8.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 9.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 1.1e1) <= 10e-9) OR ( (`dt_last_Invoice` IS NULL ) ) ) AND ( ( `CLV_SQL`  >= -2.3859999999999999e1 AND `CLV_SQL`  <= 2.7060000000000002e2 ) OR  ( `CLV_SQL`  > 2.8797000000000003e2 AND `CLV_SQL`  <= 3.0750999999999999e2 ) OR ( (`CLV_SQL` IS NULL ) ) ) ) THEN 8
		WHEN ( ((abs(month(`dt_last_Invoice`) - 3.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 4.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 5.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 6.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 7.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 8.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 9.0e0) <= 10e-9) ) AND ( ( `avg_revenue`  >= 1.0e0 AND `avg_revenue`  <= 2.1870000000000001e1 ) ) AND ((abs(day(`dt_first_Invoice`) - 9.0e0) <= 10e-9) OR (abs(day(`dt_first_Invoice`) - 1.0e1) <= 10e-9) OR  ( day(`dt_first_Invoice`)  >= 1.1e1 AND day(`dt_first_Invoice`)  <= 1.2e1 ) OR (abs(day(`dt_first_Invoice`) - 1.3e1) <= 10e-9) OR (abs(day(`dt_first_Invoice`) - 1.4e1) <= 10e-9) OR (abs(day(`dt_first_Invoice`) - 1.5e1) <= 10e-9) OR (abs(day(`dt_first_Invoice`) - 1.6e1) <= 10e-9) OR  ( day(`dt_first_Invoice`)  >= 1.9e1 AND day(`dt_first_Invoice`)  <= 2.0e1 ) OR  ( day(`dt_first_Invoice`)  >= 2.1e1 AND day(`dt_first_Invoice`)  <= 2.5e1 ) OR  ( day(`dt_first_Invoice`)  >= 2.6e1 AND day(`dt_first_Invoice`)  <= 3.1e1 ) OR ( (`dt_first_Invoice` IS NULL ) ) ) AND ((abs(`frequency` - 1.0e0) <= 10e-9) OR (abs(`frequency` - 2.0e0) <= 10e-9) ) ) THEN 7
		WHEN ( ( ( `recency_dt`  >= 3.0e0 AND `recency_dt`  <= 2.5e1 ) OR  ( `recency_dt`  > 3.1e1 AND `recency_dt`  <= 3.6e1 ) OR  ( `recency_dt`  > 3.25e2 AND `recency_dt`  <= 3.74e2 ) ) AND ((abs(`frequency` - 1.0e0) <= 10e-9) OR (abs(`frequency` - 2.0e0) <= 10e-9) OR (abs(`frequency` - 3.0e0) <= 10e-9) OR (abs(`frequency` - 4.0e0) <= 10e-9) OR (abs(`frequency` - 5.0e0) <= 10e-9) OR (abs(`frequency` - 6.0e0) <= 10e-9) OR ( (`frequency` IS NULL ) ) ) AND ( ( (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  > 3.08e2 AND (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  <= 3.4e2 ) OR  ( (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  >= 3.41e2 AND (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  <= 3.57e2 ) ) ) THEN 5
		WHEN ( ( ( (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  >= 4.0e0 AND (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  <= 3.19e2 ) OR ( (`dt_last_Invoice` IS NULL ) ) ) AND ( ( `CLV_SQL`  >= -2.3859999999999999e1 AND `CLV_SQL`  <= 3.8501999999999998e2 ) OR ( (`CLV_SQL` IS NULL ) ) ) AND ((abs(year(`dt_first_Invoice`) - 2.011e3) <= 10e-9) ) ) THEN 4
		ELSE 11
		END ) AS kc_monetary_value
    FROM {spark_ml_table}
"""
    return text_sql_ml2


sdf_ml = spark.sql(ml_sql_prediction())
s3_export_file = output_location
sdf_ml.write.mode('overwrite').parquet(s3_export_file+'Customer_CLV.parquet')


## Summary report
sdf_ml.createOrReplaceTempView('TB_CLV_SDF_ML')

ml_rpt_sql = """
WITH TB_CLUSTER AS 
(
    select kc_monetary_value as cluster_number
    , count(distinct customer_id) as customer_count
    , avg(clv_sql) avg_clv
    , avg(monetary_value) avg_monetary_value
    -- , count(*) as qty_records
    FROM TB_CLV_SDF_ML
    group by kc_monetary_value
)
SELECT cluster_number
--    , customer_count
    , ROUND( customer_count / (select sum(customer_count) from TB_CLUSTER ) * 100, 2) as percent_of_customers
    , ROUND( avg_clv, 2) as avg_clv
    , ROUND( avg_monetary_value, 2) as avg_monetary_value
FROM TB_CLUSTER tb1
order by avg_clv desc
"""

sdf_ml_rpt = spark.sql(ml_rpt_sql) 

## Export as parquet file
sdf_ml_rpt.write.mode('overwrite').parquet(rpt_location+'RPT_Customer_CLV.parquet')


# ### Done