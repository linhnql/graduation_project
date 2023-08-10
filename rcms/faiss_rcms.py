from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, size
from pyspark.sql.types import ArrayType, StringType, FloatType
from pyspark.ml.feature import Word2Vec
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline, PipelineModel
from pyvi import ViTokenizer
from pyspark.ml.feature import Word2VecModel
import faiss
import happybase
import struct
import numpy as np
import re
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RecommendationModelInference") \
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

def preprocess_text(text):
    processed_text = re.sub(r"[^\w\s]", "", text.lower().strip())
    return processed_text

def tokenize_and_extract_words(tokens):
    try:
        words = ViTokenizer.tokenize(tokens).split(" ")
        return words
    except IndexError as e:
        print("IndexError:", e)
        return []

def load_faiss_index(index_file):
    index = faiss.read_index(index_file)
    return index

def load_word2vec_model(word2vec_model_dir):
    word2vec_model = Word2VecModel.load(word2vec_model_dir)
    return word2vec_model

# labels = np.load("category_labels.npy")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Row


def load_product_recommendation_model():
    spark = SparkSession.builder.getOrCreate()
    
    model = PipelineModel.load("product_recommendation.model")
    index = faiss.read_index("product_recommendation.index")
    
    return spark, model, index

def recommend_similar_products(product_name):
    processed_name = preprocess_text(product_name)
    
    spark = SparkSession.builder.getOrCreate()
    product_df = spark.createDataFrame([(1, processed_name)], ["id", "name"])
    
    spark, model, index = load_product_recommendation_model()
    
    processed_df = model.transform(product_df)
    normalized_vectors = processed_df.select("normalized_vectors").collect()[0][0]
    vector = np.array(normalized_vectors).astype('float32').reshape(1, -1)
    
    k = 100  
    _, indices = index.search(vector, k)
    similar_product_names = [processed_df.select("name").collect()[index][0] for index in indices[0]]
    
    return similar_product_names

kmeans_file = "kmeans_index.faiss"
kmeans_index = load_faiss_index(kmeans_file)
flatL2_file = "flatL2_index.faiss"
flatL2_index = load_faiss_index(flatL2_file)

word2vec_model_dir = "word2vec_model_kmeans"
word2vec_model = load_word2vec_model(word2vec_model_dir)


# connection = happybase.Connection(host='localhost', port=9090)
# table_name = 'product'
# product_data = read_data_from_hbase(connection, table_name)
data = spark.createDataFrame(product_data)

#while True:
#    product_name = input("Enter a product name (or press Enter to quit): ")
#    if not product_name:
#        break

#    similar_products = get_similar_products(product_name, kmeans_index, faltL2_index, data, word2vec_model)
#    print("List Recommendation Product")
#    print(similar_products)
product_name = "tai nghe nhét tai audio technica athckx5is"
vector_size = 200

#similar_products = get_similar_products(product_name, kmeans_index, flatL2_index, data, word2vec_model, vector_size)
# print("Danh sách sản phẩm gợi ý:")
print(recommend_similar_products(product_name))
