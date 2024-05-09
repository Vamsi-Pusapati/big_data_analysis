import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
     .master("local") \
     .appName("Twitter Data Analysis") \
     .getOrCreate()

from pyspark.sql.readwriter import DataFrameReader
dfr = DataFrameReader(spark)
data = dfr.json(path='./data/out.json')
data.createOrReplaceTempView('tweets')

query = """
SELECT LOWER(hashtag.text) AS hashtag, COUNT(*) AS count
FROM tweets
LATERAL VIEW EXPLODE(entities.hashtags) AS hashtag
GROUP BY LOWER(hashtag.text)
ORDER BY count DESC
LIMIT 25
"""
top_hashtags = spark.sql(query)

output_path = "./output/top_hashtags"
top_hashtags.write.csv(output_path, header=True, mode="overwrite")

import pandas as pd
import plotly.express as px

top_hashtags_df = spark.read.csv("./output/top_hashtags", header=True, inferSchema=True)
df = top_hashtags_df.toPandas()

import plotly.express as px


fig = px.bar(
    df,
    x='hashtag',
    y='count',
    title='Top 25 Hashtags on Twitter',
    labels={'count': 'Number of Tweets', 'hashtag': 'Hashtag'},
    color='hashtag',
    height=600,
    text='count'
)


fig.update_layout(
    xaxis_title="Hashtags",
    yaxis_title="Tweet Count",
    xaxis_tickangle=-45,
    plot_bgcolor='white',
    showlegend=False,
    yaxis=dict(showgrid=True, gridcolor='gray'),
    xaxis=dict(showgrid=True, gridcolor='gray',categoryorder= 'total descending')
)


fig.write_image("./visualizations/top_25_hashtags.png")
