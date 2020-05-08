from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np

# Creating spark session
spark = SparkSession.builder.appName("DecisionTree App").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Loading the data
data = spark.read.format("csv").option("header", True) \
                               .option("inferSchema", True) \
                               .option("delimiter", ",") \
                               .load("D:\\UMKC\\__Spring2020\\CS5590BDP\Module-2\\Lesson-7\\MachineLearning\\data\\adult.data")


data.printSchema()

data = data.withColumn("X", F.when(F.col("X") == ' <=50K', 0).when(F.col("X") == ' >50K', 1))

data = data.withColumnRenamed("X", "label")
data = data.select(data.label.cast("double"),"age", "education-num", "hours-per-week")
data.show()

assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
data = assembler.transform(data)
data.show()

# Splitting the data into training and data set
training, test = data.select("label","features").randomSplit([0.70, 0.30])

# Create Random Forest model and fit the model with training dataset
rf = RandomForestClassifier()
model = rf.fit(training)



# Generate prediction from test dataset
pred = model.transform(test)

# Evaluate  the accuracy of the model
evaluator = MulticlassClassificationEvaluator()
accuracy = evaluator.evaluate(pred)

# Show model accuracy
print("Accuracy:", accuracy)

# Report
predictionAndLabels = pred.select("prediction", "label").rdd
metrics = MulticlassMetrics(predictionAndLabels)
print("Confusion Matrix:", metrics.confusionMatrix())
print("Precision:", metrics.precision())
print("Recall:", metrics.recall())
print("F-measure:", metrics.fMeasure())