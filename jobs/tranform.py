#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql.types import ArrayType,StringType,FloatType
import ast
from pyspark.sql.functions import explode,col


# In[ ]:


from pyspark.sql.session import SparkSession
import pyspark.pandas as ps
import openpyxl
import pandas as pd
from pyspark.sql.functions import split
spark=SparkSession.builder.appName('test').config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.0").getOrCreate()


# In[ ]:

print('read it 1')
df=ps.read_excel('jobs/maharastra-AC-2019.xlsx')
print('read it')

# In[ ]:


df=df.to_spark()


# In[ ]:


df=df.drop('Unnamed: 0')


# In[ ]:




# In[ ]:


#df.printSchema()


# In[ ]:



# ### Party vote share

# In[ ]:


#display(spark.sql('select count(Party),Party from elections group by Party'))


# ## Seat Types

# In[ ]:


#display(spark.sql('select Type,count(Type) from elections group by Type'))


# ### No.of seats of a party by regions

# In[ ]:


#display(spark.sql('select Party,Region,count(AC) from elections group by Party,Region'))


# In[ ]:


df1=df


# In[ ]:


#display(spark.sql('select Party,Type,count(Party) from elections group by Party,Type'))


# In[ ]:


#df.printSchema()


# In[ ]:


age=df.select('Age')
#display(age)


# In[ ]:


def parse_array(col):
    return ast.literal_eval(col)

parse_array_udf = udf(parse_array, ArrayType(ArrayType(StringType())))

# Apply UDF to convert string to array
# df_with_array = df.withColumn("Age", parse_array_udf(df["Age"]))

# # Show result


# # In[ ]:


# #df_with_array.printSchema()


# # In[ ]:


# #display(df_with_array)


# # In[ ]:


# df_with_array = df_with_array.withColumn("Age", explode(df_with_array["Age"]))


# In[ ]:


#display(df_with_array)


# In[ ]:


df_candidates = df.withColumn("Candidates", parse_array_udf(df["Candidates"]))
df_candidates=df_candidates.withColumn("Candidates", explode(df_candidates["Candidates"]))


# In[ ]:


candidate_names=[]
candidate_party=[]
candidate_votes=[]
vote_percent=[]
for row in df_candidates.select('Candidates').collect():
    for i in range (len(row)):

        candidate_names.append(row[i][0])
        candidate_party.append(row[i][1])
        candidate_votes.append(int(row[i][2]))
        vote_percent.append(float(row[i][3]))


# In[ ]:


candidate_ac=df_candidates.select('AC').collect()
candidate_ac = [row.AC for row in candidate_ac]


# In[ ]:


data = list(zip(candidate_ac, candidate_names,candidate_party,candidate_votes,vote_percent))
# Define column names
columns = ["AC", "Name","Party","Votes_Received","%Votes"]

# Create DataFrame
sample_df = spark.createDataFrame(data, columns)


# In[ ]:


#sample_df.show(6)


# In[ ]:


#sample_df.printSchema()


# In[ ]:


#sample_df.describe().show()


# In[ ]:




# In[ ]:


#display(spark.sql('select  distinct AC,max(Votes_Received)over (partition by AC) as `Votes Received`,first_value(Party) over (partition by AC order by Votes_Received desc) as `Winning Party` from candidates;'))


# In[ ]:


candidates=sample_df
#candidates.show()


# In[ ]:


df1=df.drop('Counting Date','other','AC Name')


# In[ ]:


def convert_to_int(value):
    return float(trim(value.split("()")))
convert_to_int_udf = udf(convert_to_int, FloatType())

# Apply the UDF to the relevant columns
df1 = df.withColumn("Total Electors", convert_to_int_udf(col("Total Electors")))
#df1 = df.withColumn("Adjusted Nett Gross", convert_to_int_udf(col("Adjusted Nett Gross")))

df1.write.csv('jobs/mahaelections.csv')
candidates.write.csv('jobs/candidates.csv')
spark.stop()

