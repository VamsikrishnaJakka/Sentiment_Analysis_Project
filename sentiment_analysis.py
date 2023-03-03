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

# create SparkSession
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

# read JSON data from HDFS location
df = spark.read.json("hdfs://localhost:50075/internship_data/amazon.json")

#converting the data type of these columns
df = df.withColumn("helpful_no", df["helpful_no"].cast(IntegerType()))
df = df.withColumn("day_diff", df["day_diff"].cast(IntegerType()))
df = df.withColumn("helpful_yes", df["helpful_yes"].cast(IntegerType()))
df = df.withColumn("overall", df["overall"].cast(IntegerType()))
df = df.withColumn("score_average_rating", df["score_average_rating"].cast(IntegerType()))
df = df.withColumn("score_pos_neg_diff", df["score_pos_neg_diff"].cast(IntegerType()))
df = df.withColumn("total_vote", df["total_vote"].cast(IntegerType()))
df = df.withColumn("wilson_lower_bound", df["wilson_lower_bound"].cast(IntegerType()))

# sort data by 'wilson_lower_bound' in descending order
df = df.sort('wilson_lower_bound', ascending=False)

# function to perform missing value analysis
def missing_value_analysis(df):
    na_columns_ = [col for col in df.columns if df[col].isNull().sum() > 0]
    n_miss = df.select([count(col).alias(col) for col in na_columns_]).collect()[0]
    ratio_ = [(n_miss[i] / df.count())*100 for i in range(len(na_columns_))]
    missing_df = spark.createDataFrame(zip(na_columns_, n_miss, ratio_), 
                                       schema=['Column Name', 'Missing Values', 'Ratio (%)'])
    return missing_df

# function to check dataframe characteristics
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

# function to check unique classes in each variable
def check_class(df):
    nunique_df = df.agg(*[udf(lambda x: len(set(x)))(col).alias(col) 
                          for col in df.columns]).toDF(*df.columns)
    nunique_df = nunique_df.selectExpr(*["'{}' as Variable".format(col) if 
                                          col != 'index' else col for col in nunique_df.columns])
    nunique_df = nunique_df.sort(nunique_df.columns[1], ascending=False).drop('index')
    return nunique_df

# function to generate countplot and percentage plot for categorical variables
def categorical_variable_summary(df, column_name, constraints):
    fig = make_subplots(rows=1, cols=2, subplot_titles=('Countplot', 'Percentage'),
                        specs=[[{"type": "xy"},{"type": 'domain'}]])
    df_ = df.groupBy(column_name).count().sort(column_name)
    fig.add_trace(go.Bar(y=df_.select('count').rdd.flatMap(lambda x: x).
