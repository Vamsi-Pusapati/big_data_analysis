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
SELECT
    t.source AS source_device,
    t.lang AS tweet_language,
    AVG(t.favorite_count) AS avg_favorites,
    AVG(SIZE(t.entities.hashtags)) AS avg_hashtag_count
FROM
    tweets t
GROUP BY
    t.source, t.lang
HAVING
    AVG(SIZE(t.entities.hashtags)) > 0
ORDER BY
    avg_favorites DESC
"""

device_data =spark.sql(query)

output_path = "./output/device_data"
device_data.write.csv(output_path, header=True, mode="overwrite")

device_data = spark.read.csv("./output/device_data", header=True, inferSchema=True)

device_lang_df = device_data.toPandas()

import plotly.express as px

# Create a bubble chart
fig = px.scatter(
    device_lang_df,
    x='tweet_language',
    y='avg_favorites',
    size='avg_hashtag_count',  
    color='source_device',  
    hover_name='source_device',  
    title='Average Favorites vs. Hashtag Count by Source and Language'
)


fig.update_layout(
    xaxis_title='Tweet Language',
    yaxis_title='Average Favorites',
    xaxis=dict(type='category'),  
    legend_title='Source Device'
)

fig.write_image("./visualizations/device_lang.png")

