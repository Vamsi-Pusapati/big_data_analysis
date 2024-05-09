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
    place.country AS country,
    media.type AS media_type,
    COUNT(*) AS media_count
FROM
    tweets
LATERAL VIEW EXPLODE(extended_entities.media) as media
WHERE
    place.country IS NOT NULL
    AND media.type IS NOT NULL
GROUP BY
    country, media_type
ORDER BY
    country
"""
media_data =spark.sql(query)

output_path = "./output/media_data"
media_data.write.csv(output_path, header=True, mode="overwrite")

media_data = spark.read.csv("./output/media_data", header=True, inferSchema=True)

media_data_df = media_data.toPandas()
import plotly.express as px

# Create the heatmap
fig = px.density_heatmap(
    media_data_df,
    x='country',
    y='media_type',
    z='media_count',
    color_continuous_scale='speed',  
    title='Media Types Shared by Country'
)

fig.update_layout(
    xaxis_title='Country',
    yaxis_title='Media Type'
)

fig.write_image("./visualizations/Media_type_by_country.png")
