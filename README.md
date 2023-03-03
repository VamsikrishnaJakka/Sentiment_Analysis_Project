# Sentiment_Analysis_Project

The use case is for a company that receives a large volume of customer reviews on a regular basis. The company may want to analyze the sentiment of the reviews to gain insights into customer satisfaction and identify areas for improvement. By using a scalable pipeline with Spark to read and store the reviews in HDFS, the company can efficiently process and analyze the data. Additionally, scheduling the pipeline to run iteratively after each hour ensures that the analysis is up-to-date and can provide real-time feedback on customer satisfaction.


# Tech Stack used

1.AWS Lambda 

2.AWS S3

3.Hadoop

4.Spark

# Architecture
![image](https://user-images.githubusercontent.com/58679637/222682521-771e4588-a4cf-496d-a8ee-8ebacd3fa423.png)


# Architecture explanation

1.AWS Lambda will be triggered by an S3 Event Notification configured on the internship/csv_input/ S3 bucket. The Lambda function will read the CSV file, transform it to JSON format, and store the resulting file in the internship/json_output/ bucket.

2.The data will be stored in the internship/json_output/ S3 bucket, which will serve as the data source for the Spark job.

3.We can use Apache Spark's built-in support for S3 to read the JSON file directly from the S3 bucket. We can use the SparkSession.builder to create a SparkSession and configure it with the necessary S3 credentials.

4.Once we have loaded the data into a Spark DataFrame, we can use PySpark's MLlib library to perform the sentiment analysis. 

5.Finally, we can save the resulting image to HDFS using the write method of the DataFrameWriter


# Detailed Procedure
