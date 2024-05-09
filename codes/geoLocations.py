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
SELECT place.full_name AS place_name,
       place.bounding_box.coordinates[0][0][1] AS latitude,
       place.bounding_box.coordinates[0][0][0] AS longitude
FROM tweets
WHERE place IS NOT NULL
"""

place_coordinates = spark.sql(query)

output_path = "./output/place_coordinates"
place_coordinates.write.csv(output_path, header=True, mode="overwrite")


fig = px.scatter_geo(
    df,
    lat='latitude',
    lon='longitude',
    hover_name='place_name',    
    projection="natural earth",
    color_discrete_sequence=['lightgreen'],
    title="Tweets Geo Locations"
)

fig.update_layout(
    geo=dict(
        showland=True,
        landcolor="peachpuff",
        showcountries=True,
        countrycolor="black"
    )
)

fig.write_image("./visualizations/twitter_geo_map.png")
