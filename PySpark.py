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

print('df1')

df1.show()

rdd2 = sc.parallelize(dataset2)

df2 = spark.createDataFrame(rdd2)

print('df2')

df2.show()