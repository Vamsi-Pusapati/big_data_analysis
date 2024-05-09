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


# Spark SQL Query to extract the count of tweets per source
query = """
SELECT source, COUNT(*) AS tweet_count
FROM tweets
GROUP BY source
ORDER BY tweet_count DESC
"""


source_distribution = spark.sql(query)

output_path = "./output/source_distributions"
source_distribution.write.csv(output_path, header=True, mode="overwrite")

source_distribution_df = spark.read.csv("./output/source_distributions", header=True, inferSchema=True)

source_distribution_pd = source_distribution_df.toPandas()

source_distribution_pd['cleaned_source'] = source_distribution_pd['source'].str.split('rel="nofollow">').str[-1].str.split('<').str[0]

source_distribution_pd.sort_values(by='tweet_count', ascending=False, inplace=True)


top_10_sources = source_distribution_pd[:10].copy()


others_sum = source_distribution_pd[10:]['tweet_count'].sum()


others_row = pd.DataFrame({'cleaned_source': ['Others'], 'tweet_count': [others_sum]})


final_df = pd.concat([top_10_sources, others_row])


import plotly.express as px


fig = px.pie(
    final_df,
    values='tweet_count',
    names='cleaned_source',
    title='Top 10 Distribution of Tweets by Source',
    color_discrete_sequence=px.colors.sequential.RdBu  
)


fig.update_traces(textposition='inside', textinfo='percent+label')
fig.update_layout(legend_title_text='Source')

fig.write_image("./visualizations/top_10_sources.png")
