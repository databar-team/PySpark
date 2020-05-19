from pyspark.sql import SparkSession# create sparksession
spark = SparkSession \
    .builder \
    .appName("Pysparkexample") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

#EXAMPLE

#example1 = spark.read.csv("example/path", header=True)
#example2 = spark.read.csv("example/path", header=True)
#example1.show()
#example2.show()



#SOME DATA
dataset1 = [
  {
  'key' : 'abc',
  'val11' : 1.1,
  'val12' : 1.2
  },
  {
  'key' : 'def',
  'val11' : 3.0,
  'val12' : 3.4
  }
]
dataset2 = [
  {
  'key' : 'abc',
  'val21' : 2.1,
  'val22' : 2.2
  },
  {
  'key' : 'xyz',
  'val21' : 3.1,
  'val22' : 3.2
  }
]

rdd1 = sc.parallelize(dataset1)
df1 = spark.createDataFrame(rdd1)
df1.show()


rdd2 = sc.parallelize(dataset2)
df2 = spark.createDataFrame(rdd2)
df2.show()

#JOIN EXAMPLES
df= df1.join(df2, on=['key'], how='inner')
df.show()

df = df1.join(df2, on=['key'], how='outer')
df.show()

df = df1.join(df2, on=['key'], how='left')
df.show()

df = df1.join(df2, on=['key'], how='right')
df.show()

#SQL JOIN
df1.createOrReplaceTempView("Table1")
df2.createOrReplaceTempView("Table2")

spark.sql(
"""
SELECT Table1.*,Table2.val21,Table2.val22 FROM Table1
INNER JOIN Table2
ON
Table1.key=Table2.key
"""
).show()

