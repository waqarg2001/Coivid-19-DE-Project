# Databricks notebook source
test_df=spark.read.option('header',True).csv('/mnt/covid19deprojectdl/raw/ecdec/testing/testing.csv')
country_lookup_df=spark.read.option('header',True).csv('/mnt/covid19deprojectdl/lookup/dim_country/country-lookup.csv')
dim_date_df=spark.read.option('header',True).option('inferSchema',True).csv('/mnt/covid19deprojectdl/lookup/dim_data/dim_data.csv')


# COMMAND ----------

from pyspark.sql.functions import lit, col, concat,to_date
dim_date_df = dim_date_df.withColumn('dim_year_week', concat(col('year').cast('string'), lit('-W'), col('week_of_year').cast('string')))


# COMMAND ----------

test_df1=test_df.join(dim_date_df, on=test_df['year_week']==dim_date_df['dim_year_week'], how='inner')


# COMMAND ----------

test_df2=test_df1.join(country_lookup_df,test_df1['Country']==country_lookup_df['Country'],how='inner')


# COMMAND ----------

test_df=test_df2.select([test_df1.country,'country_code_2_digit','country_code_3_digit',test_df1.population,'level','date','new_cases','tests_done','testing_rate','positivity_rate','testing_data_source','dim_year_week'])

# COMMAND ----------

from pyspark.sql.functions import when, col

# Replace 'NA' with NULL in the 'tests_done' column
test_df = test_df.withColumn('tests_done', when(col('tests_done') == 'NA', None).otherwise(col('tests_done').cast('int')))
test_df = test_df.withColumn('testing_rate', when(col('testing_rate') == 'NA', None).otherwise(col('testing_rate').cast('float')))
test_df = test_df.withColumn('positivity_rate', when(col('positivity_rate') == 'NA', None).otherwise(col('positivity_rate').cast('float')))
test_df = test_df.withColumn('testing_data_source', when(col('testing_data_source') == 'NA', None).otherwise(col('testing_data_source').cast('string')))
test_df = test_df.withColumn('new_cases', when(col('new_cases') == 'NA', None).otherwise(col('new_cases').cast('int')))


# COMMAND ----------

display(test_df)

# COMMAND ----------

from pyspark.sql.functions import min as min_spark,max as max_spark
result_df = test_df.groupBy(
    'country',
    'country_code_2_digit',
    'country_code_3_digit',
    'population',
    'level',
    'new_cases',
    'tests_done',
    'testing_rate',
    'positivity_rate',
    'testing_data_source',
    'dim_year_week'
).agg(
    min_spark('date').alias('week_start_date'),
    max_spark('date').alias('week_end_date')
)

# COMMAND ----------

result_df.write.mode('overwrite').csv('/mnt/covid19deprojectdl/processed/ecdc/testing',header=True)

# COMMAND ----------

