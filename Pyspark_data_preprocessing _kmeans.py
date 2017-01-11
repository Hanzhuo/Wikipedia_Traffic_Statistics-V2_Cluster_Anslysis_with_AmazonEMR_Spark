
#read one week uncompressed files
files=sc.textFile("s3://678wikiclusterdata/678wikiclusterdata/")
#Process data and convert to dataframe
x =  files.map(lambda line: line.split(" "))
df = x.toDF(['project', 'name', 'views', 'bytes'])

#convert project names (string) to numbers (int)
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="project", outputCol="project_index", handleInvalid='error')
model = stringIndexer.fit(df)
td = model.transform(df)
td.show(5)



td.registerTempTable("dfData")
#use sql group by project_index and name to calculate sum of views and mean of bytes for each page 
viewset = spark.sql("SELECT project_index, name, sum(views) AS views, avg(bytes) AS bytes FROM dfData GROUP BY project_index, name")
#check the statistical description
#viewset.describe('views','bytes').show()


viewset.registerTempTable("dfview")
#get the mean value of views
highview = spark.sql("SELECT * FROM dfview WHERE views > 19.75")
#highview.show(5)


name_inputNum=sc.textFile("s3://678wikiclusterdata/name_inputNum.txt")
x =  name_inputNum.map(lambda line: line.split(" "))
name_input_df = x.toDF(['name', 'inputNum'])

#combine highview and name_input_df dataframes with sql join function
finalDF = highview.join(name_input_df, 'name', 'left').select(highview.name, highview.views, highview.bytes, name_input_df.inputNum, highview.project_index)
#replace null value with 0
finalDF = finalDF.na.fill({'inputNum': 0})
finalDF.show(5)


finalDF.describe('views','bytes', 'inputNum', 'project_index').show()


finalDF1 = finalDF


from pyspark.sql.functions import min
from pyspark.sql.functions import max
views_min = finalDF1.select(min('views').alias('min_views')).collect()[0]['min_views']
views_max = finalDF1.select(max('views').alias('max_views')).collect()[0]['max_views']

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
Min_v = views_min
Max_v = views_max
#user define function
Norm_views_function = udf(lambda v: (v - Min_v) / (Max_v - Min_v), DoubleType())
finalDF2 = finalDF1.withColumn('Norm_views', Norm_views_function(finalDF1.views))
#Norm_views = finalDF1.select(Norm_views_function(finalDF1.views).alias('Norm_views'))

bytes_min = finalDF1.select(min('bytes').alias('min_bytes')).collect()[0]['min_bytes']
bytes_max = finalDF1.select(max('bytes').alias('max_bytes')).collect()[0]['max_bytes']
Min_v = bytes_min
Max_v = bytes_max
Norm_bytes_function = udf(lambda v: (v - Min_v) / (Max_v - Min_v), DoubleType())
finalDF2 = finalDF2.withColumn('Norm_bytes', Norm_bytes_function(finalDF2.bytes))

inputNum_min = float(finalDF1.select(min('inputNum').alias('min_inputNum')).collect()[0]['min_inputNum'])
inputNum_max = float(finalDF1.select(max('inputNum').alias('max_inputNum')).collect()[0]['max_inputNum'])
Min_v = inputNum_min
Max_v = inputNum_max
Norm_inputNum_function = udf(lambda v: (float(v) - Min_v) / (Max_v - Min_v), DoubleType())
finalDF2 = finalDF2.withColumn('Norm_inputNum', Norm_inputNum_function(finalDF2.inputNum))



#conduct OneHotEncoder for project_index column
from pyspark.ml.feature import OneHotEncoder

finalDF2.registerTempTable("dfData")
finalDF2 = spark.sql("SELECT name, Norm_views, Norm_bytes, Norm_inputNum, project_index FROM dfData")

encoder = OneHotEncoder(dropLast=False, inputCol="project_index", outputCol="project_Vec")
encoded = encoder.transform(finalDF2)


encoded.registerTempTable("dfData")
finalDF3 = spark.sql("SELECT Norm_views, Norm_bytes, Norm_inputNum, project_Vec FROM dfData")

from pyspark.mllib.linalg import SparseVector
import numpy as np
#824 should be revised according to your onehotcode result
RDD = finalDF3.rdd.map(lambda line: SparseVector(824, line["project_Vec"].indices.tolist() + [821, 822, 823], line["project_Vec"].values.tolist() + [line["Norm_views"], line["Norm_bytes"], line["Norm_inputNum"]])).cache()


from pyspark.mllib.clustering import KMeans
clusters = KMeans.train(RDD, 2, maxIterations=10, runs=10, initializationMode="k-means||")