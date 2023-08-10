from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, size
from pyspark.sql.types import ArrayType, StringType, FloatType
from pyspark.ml.feature import Word2Vec
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyvi import ViTokenizer, ViUtils
import faiss
import happybase
import struct
import numpy as np
import re


# product_data = [
#     {'id': 1, 'name': 'tai nghe nhét tai audio technica athckx5is'},
#     {'id': 2, 'name': 'tai nghe nhét tai audio technica athchx5is'},
#     {'id': 3, 'name': 'tai nghe chụp tai audio technica athm50x'},
#     {'id': 4, 'name': 'hộp đựng thực phẩm chữ nhật kova colorful life 750ml'},
#     {'id': 5, 'name': 'tai nghe nhét tai sony fontopia mdre9lp'},
#     {'id': 6, 'name': 'tai nghe chụp tai sony mdrzx110ap'},
#     {'id': 7, 'name': 'tai nghe nhét tai sony mdrex15ap'},
#     {'id': 8, 'name': 'hộp cơm điện hâm nóng đa năng magic a03 105l'},
#     {'id': 9, 'name': 'loa bluetooth isound sp12 16w'},
#     {'id': 10, 'name': 'máy massage mini cầm tay beurer mg16'},
#     {'id': 11, 'name': 'tai nghe urbanista san francisco'},
#     {'id': 12, 'name': 'loa di động microlab t6 36w'},
#     {'id': 13, 'name': 'chuột quang không dây genius nx 7000'},
#     {'id': 14, 'name': 'chuột không dây genius nx7015'},
#     {'id': 15, 'name': 'chuột không dây forter v181'},
#     {'id': 16, 'name': 'ngủ ngon hẹn mai nhé'},
#     {'id': 17, 'name': 'tai nghe nhét tai audio technica athcks770is'},
#     {'id': 18, 'name': 'tai nghe chụp tai audio technica athws550is'},
#     {'id': 19, 'name': 'chuột không dây logitech m325'},
#     {'id': 20, 'name': 'tủ tabi duy tân 5 ngăn họa tiết ngẫu nhiên'}
# ]


def preprocess_text(text):
    processed_text = re.sub(r"[^\w\s]", "", text.lower().strip())
    processed_text = (processed_text.replace("hàng chính hãng", "")
                      .replace("chính hãng", "")
                      .replace("hàng nhập khẩu", "")
                      .replace("nhập khẩu", "")
                      .replace("chi tiết", "")
                      .replace("phân phối", ""))
    return processed_text


def read_data_from_hbase(connection, table_name):
    table = connection.table(table_name.encode('utf-8'))
    data = []
    a = 1
    for key, value in table.scan():
        product = {}
        product['id'] = struct.unpack('!i', key)[0]
        product['name'] = preprocess_text(value[b'basic_info:name'].decode('utf-8'))

        data.append(product)
        print(a)
        a += 1

    return data


def tokenize_and_extract_words(text):
    try:
        # words = ViUtils.remove_accents(ViTokenizer.tokenize(text)).decode("utf-8").split(" ")
        words = ViTokenizer.tokenize(text).split(" ")
        return words
    except IndexError as e:
        print("IndexError:", e)
        return []


def train_recommendation_model(connection, table_name):
    product_data = read_data_from_hbase(connection, table_name)

    spark = SparkSession.builder \
        .appName("RecommendationModelTraining") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    product_df = spark.createDataFrame(product_data)

    tokenizer = udf(tokenize_and_extract_words, ArrayType(StringType()))
    product_df = product_df.withColumn("tokens", tokenizer("name"))
    print("Number of rows before filtering empty tokens:", product_df.count())

    product_df = product_df.filter(size("tokens") > 0)
    print("Number of rows after filtering empty tokens:", product_df.count())

    word2vec = Word2Vec(vectorSize=100, seed=42, inputCol="tokens",
                        outputCol="features_word2vec", minCount=5)
    word2vec_model = word2vec.fit(product_df)
    word2vec_model.write().overwrite().save("word2vec_model")
    product_df = word2vec_model.transform(product_df)

    features = np.array(product_df.select("features_word2vec")
                        .rdd.map(lambda x: x[0]).collect(), dtype=np.float32)

    faiss.normalize_L2(features)

    index = faiss.IndexFlatL2(100)
    index.add(features)

    faiss.write_index(index, "vector_index.faiss")


connection = happybase.Connection(host='localhost', port=9090)
table_name = 'product'
train_recommendation_model(connection, table_name)
