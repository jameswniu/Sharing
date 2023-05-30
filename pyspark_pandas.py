"""Implement SQL in Pandas"""
import pandas as pd


# get mean of salary by department
avg_dpt_salary = df.groupby('Department')['Salary'].transform('mean')

# create a masking df
df_filtered = df[df['Salary'] > avg_dpt_salary]

# apply masking df to entire df
df_filtered = df_filtered[['EmpName', 'Salary']]


"""Implement SQL in PySpark"""
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col


# start spark session
spark = SparkSession.builder.getOrCreate()

# specify partitioning and group by department
windowSpec = Window.partitionBy('Department')
df = df.withColumn('Avg_Dpt_Salary', avg(df['Salary']).over(windowSpec))

# selecting conditional employees
df_filtered = df.filter(col('Salary') > col('Avg_Dpt_Salary')).select('EmpName', 'Salary')