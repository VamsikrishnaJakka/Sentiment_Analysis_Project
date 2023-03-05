#Step 1: Importing Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, StructField
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import NaiveBayes
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re
from plotly.subplots import make_subplots
import plotly.graph_objects as go

#Step 2: Create SparkSession
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

#Step 3: Read JSON data from HDFS location
df = spark.read.json("hdfs://localhost:50075/internship_data/amazon.json")

#Step 4: Converting Data Types
df = df.withColumn("helpful_no", df["helpful_no"].cast(IntegerType()))
df = df.withColumn("day_diff", df["day_diff"].cast(IntegerType()))
df = df.withColumn("helpful_yes", df["helpful_yes"].cast(IntegerType()))
df = df.withColumn("overall", df["overall"].cast(IntegerType()))
df = df.withColumn("score_average_rating", df["score_average_rating"].cast(IntegerType()))
df = df.withColumn("score_pos_neg_diff", df["score_pos_neg_diff"].cast(IntegerType()))
df = df.withColumn("total_vote", df["total_vote"].cast(IntegerType()))
df = df.withColumn("wilson_lower_bound", df["wilson_lower_bound"].cast(IntegerType()))


#Step 5: Sort data by 'wilson_lower_bound' in descending order
df = df.sort('wilson_lower_bound', ascending=False)

#Step 6: Function to perform missing value analysis
def missing_value_analysis(df):
    na_columns_ = [col for col in df.columns if df[col].isNull().sum() > 0]
    n_miss = df.select([count(col).alias(col) for col in na_columns_]).collect()[0]
    ratio_ = [(n_miss[i] / df.count())*100 for i in range(len(na_columns_))]
    missing_df = spark.createDataFrame(zip(na_columns_, n_miss, ratio_), 
                                       schema=['Column Name', 'Missing Values', 'Ratio (%)'])
    return missing_df

#Step 7: Function to check dataframe characteristics
def check_dataframe(df, head=5, tail=5):
    print("SHAPE".center(82,'~'))
    print("ROWS:{}".format(df.count()))
    print('COLUMNS:{}'.format(len(df.columns)))
    print('TYPES'.center(82,'~'))
    df.printSchema()
    print("".center(82,'~'))
    print(missing_value_analysis(df).show())
    print('DUPLICATED VALUES'.center(83,'~'))
    print(df.dropDuplicates().count())
    print("QUANTILES".center(82,'~'))
    df.select([col for col in df.columns if df.schema[col].dataType in 
               [IntegerType(), FloatType()]]).summary("0%", "5%", "50%", "95%", "99%", "100%").show()


# Step 8:Define UDFs for sentiment analysis
def get_textblob_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity

def get_vader_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()
    scores = analyzer.polarity_scores(text)
    return scores['compound']

#Step 9: Create UDFs
textblob_udf = udf(get_textblob_sentiment, FloatType())
vader_udf = udf(get_vader_sentiment, FloatType())

#Step 10: Add columns for sentiment analysis using UDFs
df = df.withColumn("textblob_sentiment", textblob_udf(df["reviewText"]))
df = df.withColumn("vader_sentiment", vader_udf(df["reviewText"]))

#Step 11: Create tokenizer and hashing TF objects
tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")

#Step 12: Fit the tokenizer and hashing TF objects to the data
wordsData = tokenizer.transform(df)
tfData = hashingTF.transform(wordsData)

#Step 13: Create IDF object and fit to the data
idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="tf_idf")
idfModel = idf.fit(tfData)
tfidfData = idfModel.transform(tfData)

#Step 14: Split data into training and test sets
(trainingData, testData) = tfidfData.randomSplit([0.7, 0.3], seed=100)

#Step 15: Train Naive Bayes model on training data
nb = NaiveBayes(smoothing=1)
model = nb.fit(trainingData)

#Step 16: Make predictions on test data
predictions = model.transform(testData)

#Step 17: Select necessary columns and rename for output
output = predictions.select("reviewText", "textblob_sentiment", "vader_sentiment", "prediction") \
    .withColumnRenamed("prediction", "naive_bayes_prediction")

#Step 18: Write output to HDFS
output.write.json("hdfs://localhost:50075/internship_data/amazon_sentiment_analysis")