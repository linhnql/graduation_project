from pyspark.sql.functions import desc
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.ml.linalg import Vectors
import faiss
import re
from sklearn.manifold import TSNE
from pyspark.ml.feature import Word2Vec
from pyspark.ml.feature import Normalizer
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf, size, col, desc
from pyspark.sql.types import ArrayType, StringType, FloatType, DoubleType
from pyvi import ViTokenizer, ViPosTagger
import happybase
import struct
import numpy as np
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import StandardScaler
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.patheffects as PathEffects
import seaborn as sns


def preprocess_text(text):
    processed_text = re.sub(r"[^\w\s]", "", text.lower().strip())
    return processed_text


def read_data_from_hbase(connection, table_name):
    table = connection.table(table_name.encode('utf-8'))
    data = []

    for key, value in table.scan():
        product = {}
        product['id'] = struct.unpack('!i', key)[0]
        product['name'] = (preprocess_text(value[b'basic_info:name'].decode('utf-8'))
                           .replace("hàng chính hãng", "")
                           .replace("chính hãng", "")
                           .replace("hàng nhập khẩu", "")
                           .replace("nhập khẩu", "")
                           .replace("chi tiết", "")
                           .replace("phân phối", ""))

        data.append(product)

    return data


def tokenize_and_extract_words(text):
    try:
        words = ViTokenizer.tokenize(text).split(" ")
        return words
    except IndexError as e:
        print("IndexError:", e)
        return []


def train_recommendation_model(product_df):

    tokenizer = udf(tokenize_and_extract_words, ArrayType(StringType()))
    product_df = product_df.withColumn("tokens", tokenizer("name"))
    print("Number of rows before filtering empty tokens:", product_df.count())

    product_df = product_df.filter(size("tokens") > 0)
    print("Number of rows after filtering empty tokens:", product_df.count())

    word2vec = Word2Vec(vectorSize=100, seed=42, inputCol="tokens",
                        outputCol="features_word2vec", minCount=5)
    kmeans = KMeans(k=3, seed=42)
    assembler = VectorAssembler(
        inputCols=["features_word2vec"], outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

    pipeline = Pipeline(stages=[word2vec, assembler, scaler, kmeans])
    model = pipeline.fit(product_df)
    product_df = model.transform(product_df)

    # clusters = model.stages[-1].summary.predictions
    # num_clusters = clusters.select("prediction").distinct().count()
    # cluster_colors = [matplotlib.colormaps.get_cmap("Set1")(i % 100) for i in range(num_clusters)]

    # features = product_df.select("scaledFeatures").rdd.map(lambda x: x[0]).collect()
    # cluster_labels = clusters.select("prediction").rdd.map(lambda x: x[0]).collect()

    # for i in range(len(features)):
    #     x = features[i][0]
    #     y = features[i][1]
    #     cluster_label = cluster_labels[i]
    #     color = cluster_colors[cluster_label]
    #     plt.scatter(x, y, color=color)

    # plt.show()

    return model


def recommend_similar_products(model, product_df, product_name, num_recommendations=5):
    tokenizer = udf(tokenize_and_extract_words, ArrayType(StringType()))
    tokens = tokenizer(product_name).getItem(0)

    input_df = spark.createDataFrame(
        [(product_name, tokens)], ["name", "tokens"])

    input_df = model.transform(input_df)

    input_features = input_df.select("features").first()[0]

    cosine_similarity_udf = udf(lambda features: float(Vectors.dense(input_features).dot(Vectors.dense(
        features)) / (np.linalg.norm(Vectors.dense(input_features)) * np.linalg.norm(Vectors.dense(features)))), FloatType())
    product_df = model.transform(
        product_df.withColumnRenamed("name", "product_name"))

    product_df = product_df.withColumn(
        "similarity", cosine_similarity_udf("features"))

    recommended_products = product_df.orderBy(
        desc("similarity")).limit(num_recommendations)

    recommended_products.select("product_name", "similarity").show()


spark = SparkSession.builder \
    .appName("RecommendationModelTraining") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

product_data = [
    {'id': 1, 'name': 'tai nghe nhét tai audio technica athckx5is'},
    {'id': 2, 'name': 'tai nghe nhét tai audio technica athdhx5are'},
    {'id': 3, 'name': 'tai nghe chụp tai audio technica athm50x'},
    {'id': 4, 'name': 'hộp đựng thực phẩm chữ nhật kova colorful life 750ml'},
    {'id': 5, 'name': 'tai nghe nhét tai sony fontopia mdre9lp'},
    {'id': 6, 'name': 'tai nghe chụp tai sony mdrzx110ap'},
    {'id': 7, 'name': 'tai nghe nhét tai sony mdrex15ap'},
    {'id': 8, 'name': 'loa di động microlab t8 '},
    {'id': 9, 'name': 'loa bluetooth isound sp12 16w'},
    {'id': 10, 'name': 'máy massage mini cầm tay beurer mg16'},
    {'id': 11, 'name': 'tai nghe urbanista san francisco'},
    {'id': 12, 'name': 'loa di động microlab t6 36w'},
    {'id': 13, 'name': 'chuột quang không dây genius nx 7000'},
    {'id': 14, 'name': 'chuột không dây genius nx7015'},
    {'id': 15, 'name': 'chuột không dây forter v181'},
    {'id': 16, 'name': 'ngủ ngon hẹn mai nhé'},
    {'id': 17, 'name': 'tai nghe nhét tai audio technica athcks770is'},
    {'id': 18, 'name': 'tai nghe chụp tai audio technica athws550is'},
    {'id': 19, 'name': 'chuột không dây logitech m325'},
    {'id': 20, 'name': 'tủ tabi duy tân 5 ngăn họa tiết ngẫu nhiên'},
    {'id': 21, 'name': 'tủ tomi a4 duy tân 3 ngăn'},
    {'id': 22, 'name': 'tủ tomi a4 duy tân 4 ngăn họa tiết ngẫu nhiên'},
    {'id': 23, 'name': 'tủ tomi a4 duy tân 5 ngăn họa tiết ngẫu nhiên'},
    {'id': 24, 'name': 'tủ a4 duy tân 3 ngăn nắp bằng giao mẫu ngẫu nhiên'},
    {'id': 25, 'name': 'tủ a4 duy tân 4 ngăn nắp bằng giao mẫu ngẫu nhiên'},
    {'id': 26, 'name': 'tủ a4 duy tân 5 ngăn nắp bằng giao mẫu ngẫu nhiên'},
    {'id': 27, 'name': 'chuột không dây logitech m171 hàng chính hãng'},
    {'id': 28, 'name': 'chuột không dây logitech m557 hàng chính hãng'},
    {'id': 29, 'name': 'tai nghe chụp tai audio technica athsr5 hàng chính hãng'},
    {'id': 30, 'name': 'tai nghe yison celebrat v36 nhét tai hàng chính hãng'},
    {'id': 31, 'name': 'giấc mơ mỹ đường đến stanford'},
    {'id': 32, 'name': 'chuột không dây logitech m221 hàng chính hãng'},
    {'id': 33, 'name': 'chuột không dây logitech m331 silent hàng chính hãng'},
    {'id': 34, 'name': 'bình lưỡng tính tiger mjaa048 480ml'},
    {'id': 35, 'name': 'bình lưỡng tính tiger mjca048 480ml'},
    {'id': 36, 'name': 'bình thủy chứa tiger maaa402 4 lít'},
    {'id': 37, 'name': 'tai nghe nhét tai soundmax ah306s hàng chính hãng'},
    {'id': 38, 'name': 'chuột không dây genius nx7005 hàng chính hãng'},
    {'id': 39, 'name': 'bình lưỡng tính tiger mjca048 48000ml'},
    {'id': 40, 'name': 'bình lưỡng tính tiger mjca0434 480ml'},
    {'id': 41, 'name': 'ngủ ngon hẹn mai nhé nha'},
    {'id': 42, 'name': 'loa di động microlab t6 343543w'},
    {'id': 43, 'name': 'loa  microlab t6 36w'},
    {'id': 44, 'name': 'tai nghe Bluetooth JBL T450BT'},
    {'id': 45, 'name': 'loa di động microlab t6 36w'},
    {'id': 46, 'name': 'máy massage mini cầm tay beurer 1BUDF'},
    {'id': 47, 'name': 'máy massage mini cầm tay beure 64GB'},
    {'id': 48, 'name': 'giày thể thao nữ Nike Revolution 5'},
    {'id': 49, 'name': 'máy massage mini cầm tay beurer '},
    {'id': 50, 'name': 'ngủ ngon hẹn mai kia'}
]

# connection = happybase.Connection(host='localhost', port=9090)
# table_name = 'product'
# product_data = read_data_from_hbase(connection, table_name)

product_df = spark.createDataFrame(product_data)

product_name = "tai nghe nhét tai audio technica athckx5is"
model = train_recommendation_model(product_df)
recommend_similar_products(model, product_df, product_name)
