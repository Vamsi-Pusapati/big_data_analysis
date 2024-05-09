import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
     .master("local") \
     .appName("Twitter Analysis") \
     .getOrCreate()

from pyspark.sql.readwriter import DataFrameReader
dfr = DataFrameReader(spark)
data = dfr.json(path='./data/out.json')
data.createOrReplaceTempView('tweets')

query = """SELECT full_text FROM tweets WHERE lang='en'"""

en_text =spark.sql(query)

output_path = "./output/full_text_en"
en_text.write.csv(output_path, header=True, mode="overwrite")


en_text = spark.read.csv("./output/full_text_en", header=True, inferSchema=True)


from pyspark.sql.functions import regexp_replace, lower, trim

def preprocess_text(df, text_column):
    # Remove URLs
    df = df.withColumn(text_column, regexp_replace(text_column, r"http\S+", ""))
    # Remove Twitter @mentions
    df = df.withColumn(text_column, regexp_replace(text_column, r"@\w+", ""))
    # Remove special characters and numbers
    df = df.withColumn(text_column, regexp_replace(text_column, r"[^a-zA-Z\s]", ""))
    # Remove extra spaces
    df = df.withColumn(text_column, trim(regexp_replace(text_column, r"\s+", " ")))
    return df

#spark data frame should be passed instead of pandas df
processed_en_text = preprocess_text(en_text, "full_text")

en_text_processed_df = processed_en_text.toPandas()

from pyspark.ml.feature import Tokenizer, StopWordsRemover

tokenizer = Tokenizer(inputCol="full_text", outputCol="words")
words_df = tokenizer.transform(processed_en_text)

remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
filtered_df = remover.transform(words_df)

from pyspark.sql.functions import explode, col

exploded_df = filtered_df.withColumn("word", explode(col("filtered_words")))
word_counts_df = exploded_df.groupBy("word").count()

top_words_df = word_counts_df.orderBy(col("count").desc()).limit(100)
top_words = top_words_df.collect()

from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Create a dictionary of word frequencies
word_frequencies = {word['word']: word['count'] for word in top_words}

# Generate a word cloud image
wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_frequencies)

# Display the cloud
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')

plt.savefig("./visualizations/top_100_words.png")
