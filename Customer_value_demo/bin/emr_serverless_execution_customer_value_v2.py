import sys
import os
import datetime

### Comment 3 lines below  - Sample execution with findspark - Google Colab example or local execution
# os.environ['SPARK_HOME'] = 'SPARK_HOME_DIRECTORY...spark-3.3.1-bin-hadoop2' 
# import findspark
# findspark.init()

## Spark
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType

"""
This program is designed to run on either EMR Serverless or EMR Cluster and execute a
PySpark program that calculates customer lifetime value (CLV), runs a machine learning model
using Spark-SQL, export the results and generates information as an AWS Glue database
for database marketing purposes. Additionally, it exports a small aggregated report at the end.

To run the program with EMR Cluster, you will need to:

Create an EMR cluster with the necessary configuration and security settings
Upload the customer transaction data to an S3 bucket
Submit the PySpark program to the EMR cluster using the spark-submit command or
convert this code to EMR Notebook with few changes to execute it

To run the program locally with a Parquet file sample, you will need to:

Install PySpark and configure it to work with your local environment and configure
the variables EMR_SERVERLESS_EXECUTION to False change S3_BUCKET_NAME to local filename
and finally uncomment findspark code and os.environ['SPARK_HOME'] to run locally or using
Google Colab as an example

Some details of execution

Preprocess the data and calculate the relevant customer metrics such as
recency, frequency, and monetary value

Use Spark-SQL to run a machine learning program, such as a clustering algorithm, to segment
the customers based on their transaction patterns and calculate their CLV scores

Generate a small aggregated report that summarizes the customer segments and their CLV scores

Note: local execution may need to restart Spark and delete folders
spark-warehouse
metastore_db
emr_serverless_demo/ DIR

EMR Cluster version - 6.10
Spark version - 3.3.1-amzn-0

"""

### EMR serverless execution
## Set True or False (EMR Serverless or EMR Cluster)
EMR_SERVERLESS_EXECUTION = True ## or False for EMR Cluster of local execution

S3_BUCKET_NAME = '../../emr_serverless_demo'

### Setup AWS GLUE
DBNAME_GLUE = 'DBM' ## database Marketing
TBP_TABLE_NAME = 'TBP_CUSTOMER_CLV_SERVERLESS' ## Parquet table - Customer Lifetime Value
SPARK_ML_TABLE = 'TB_ML_SPARK_SDF'

def fnc_print_datetime(msg='Default - msg'):
    """
    Function to get current date and time and print a msg

    Parameters
    ----------
    msg : TYPE, string
        DESCRIPTION. The default is 'Default - msg'.

    Returns
    -------
    None.

    """
    dt_format = datetime.datetime.now()
    formatted_date = dt_format.strftime('%Y-%m-%d %H:%M:%S')
    print(formatted_date, msg)

def fnc_customer_clv_udf(monetary_value_f, frequency_f, recency_f, discount_f=0.1):
    """
    This function implements the customer lifetime value - Spark UDF sample
    ## ## formula to calculate CLV  using SPARK-udf

    Returns
    -------
    CLV

    """
    clv_formula = round (
        ( (monetary_value_f / frequency_f) * (1 - ((recency_f + 1) / 365)) / (1 + discount_f) )
        , 2)

    return clv_formula

def fnc_validate_parameters(aws_emr_serverless_execution=EMR_SERVERLESS_EXECUTION):
    """
    This function validate the parameters to be used with EMR Serverless

    Parameters
    ----------
    aws_emr_serverless_execution : TYPE: boolean, optional  - EMR Cluster or localhost DEBUG
        DESCRIPTION. The default is EMR_SERVERLESS_EXECUTION = True.

    Returns
    -------
    Input, Output and RPT path passed as parameters or stop the process with an error

    """
    if (len(sys.argv) != 4) and aws_emr_serverless_execution:
        print("Usage: spark-etl ['input folder'] ['output folder'] ['rpt_folder']")
        sys.exit(-1)

    if not aws_emr_serverless_execution: ## EMR Cluster or local execution
        input_location_aux = S3_BUCKET_NAME + '/s3_data/input/'
        output_location_aux = S3_BUCKET_NAME + '/s3_data/output/'
        rpt_location_aux = S3_BUCKET_NAME + '/s3_data/rpt/'
    else: # EMR Serverless parameters sample
        # ['../s3_data/input/', '../s3_data/output/', '../s3_data/rpt/']
        input_location_aux = sys.argv[1]
        output_location_aux = sys.argv[2]
        rpt_location_aux = sys.argv[3]

    # ## Debug INFO
    print('Input location: ', input_location_aux)
    print('Output location: ', output_location_aux)
    print('Rpt location: ', rpt_location_aux)

    return input_location_aux, output_location_aux, rpt_location_aux


def fshape(dataframe1):
    """
    This function print the number of records and columns of a spark dataframe

    Returns
    -------
    None.

    """
    ## Function to print shape of Spark dataframe
    print('Shape : ', dataframe1.count(), len(dataframe1.columns))

def fnc_show_db_tables():
    """
    This function print databases and tables

    Returns
    -------
    None.

    """
    spark.sql( ' SHOW DATABASES ').show()
    spark.sql(' SHOW TABLES ').show()


def spark_sql_write_glue_database(db_name
                                  , table_name
                                  , parquet_output_location='glue_output_location_tmp'
                                  , temp_table ='SPARK_ML_TABLE_tmp'):
    """
    This function create the Database Marketing and the customer CLV table in AWS GLUE

    Returns
    -------
    None.

    """

    ## Function to CREATE DATABASE and TABLE - AWS Glue
    print(' database creation: ', db_name)
    spark.sql(f" CREATE database if not exists {db_name} ")
    print()
    print(' Table name creation , ', table_name)
    spark.sql((
        f" CREATE TABLE IF NOT EXISTS {db_name}.{table_name} "
        f" USING PARQUET LOCATION '{parquet_output_location}' AS SELECT * FROM {temp_table}"
    ))

## Main Program
fnc_print_datetime(msg=' - Program started  ... ')
(input_location, output_location, rpt_location) = fnc_validate_parameters()


spark = SparkSession\
    .builder\
    .appName('SparkETL')\
    .enableHiveSupport()\
    .getOrCreate()


## read local file
sdf = spark.read.parquet(input_location)

## ETL
sdf.createOrReplaceTempView('TB_SALES_SDF')
# spark.sql("""
#    select max(TO_DATE(InvoiceDate)) as current_date_for_FRMV_CLV,
#        current_date as not_today
#    from TB_SALES_SDF'
# """).show()


## Register the formula to be used by Spark-SQL
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
sdf_clv.createOrReplaceTempView(SPARK_ML_TABLE)


def ml_sql_prediction():
    """
    This Build cluster of Customers based on SQL statemeent

    Returns: SQL to be executed with Spark dataframe
    """

    text_sql_ml2 = f"""
    SELECT 
        {SPARK_ML_TABLE}.*,
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
    FROM {SPARK_ML_TABLE}
"""
    return text_sql_ml2


## Generate customer CLV with ML - cluster of customers
sdf_ml = spark.sql(ml_sql_prediction())
s3_export_file = output_location
spark_ml_customer_str = s3_export_file+'Customer_CLV.parquet'
sdf_ml.createOrReplaceTempView('TB_CLV_SDF_ML')
sdf_ml.write.mode('overwrite').parquet(spark_ml_customer_str)



## Summary report
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

## AWS Glue DATABASE AND TABLE GENERATION
### RESTORE COMMENT
spark_ml_customer_str = s3_export_file+'Customer_CLV.parquet'
spark_sql_write_glue_database(db_name=DBNAME_GLUE,
                              table_name=TBP_TABLE_NAME,
                                  ## original without cluster SPARK_ML_TABLE_tmp
                              temp_table = 'TB_CLV_SDF_ML',
                              parquet_output_location=spark_ml_customer_str)

# ## Evaluate all results - debug - result execution sample
# spark.sql(f"USE DATABASE {DBNAME_GLUE}" )
# fnc_show_db_tables()
# spark.sql(f" DESC TABLE {DBNAME_GLUE}.{TBP_TABLE_NAME} ").show()
# ## Detailed description
# # spark.sql(f" DESC EXTENDED {DBNAME_GLUE}.{TBP_TABLE_NAME} ").show()
# print(spark_ml_customer_str)
# sdf_tmp_parquet = spark.read.parquet(spark_ml_customer_str)
# sdf_tmp_parquet.printSchema()

print(' : ', )
fnc_print_datetime(msg=' - End of execution. ')
## Done
