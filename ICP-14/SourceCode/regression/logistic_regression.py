from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

from pyspark.sql import functions as F

spark = SparkSession.builder.appName("LogisticRegression App").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# Loading the data
data = spark.read.format("csv").option("header", True) \
                               .option("inferSchema", True) \
                               .option("delimiter", ",") \
                               .load("D:\\UMKC\\__Spring2020\\CS5590BDP\\Module-2\\Lesson-7\\MachineLearning\\data\\imports-85.data")


data.printSchema()

data = data.withColumn("label", F.when(F.col("num-of-doors") == "four", 1).otherwise(0)).select("label","length", "width","height")
data.show()

# Create vector assembler for feature columns
assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
data = assembler.transform(data)

lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
model = lr.fit(data)

# Print the coefficients and intercept for logistic regression
print("Coefficients: " + str(model.coefficients))
print("Intercept: " + str(model.intercept))

# We can also use the multinomial family for binary classification
mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")

# Fit the model
mlr_model = mlr.fit(data)

# Print the coefficients and intercepts for logistic regression with multinomial family
print("Multinomial coefficients: " + str(mlr_model.coefficientMatrix))
print("Multinomial intercepts: " + str(mlr_model.interceptVector))

