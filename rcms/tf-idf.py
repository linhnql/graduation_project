from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyvi import ViTokenizer, ViUtils
import happybase
import struct
import numpy as np
import re
from pyspark.ml.feature import CountVectorizer, IDF, StopWordsRemover, VectorAssembler, PCA, HashingTF
from pyspark.ml.pipeline import Pipeline
from pyspark.sql.functions import col, udf, lit, concat

def tokenize_and_extract_words(text):
    try:
        words = ViTokenizer.tokenize(text).split(" ")
        return words
    except IndexError as e:
        print("IndexError:", e)
        return []

def preprocess_text(text):
    if not text or text is None:
        return ''
    
    text = text.replace("Giá sản phẩm trên Tiki đã bao gồm thuế theo luật hiện hành. Bên cạnh đó, tuỳ vào loại sản phẩm, hình thức và địa chỉ giao hàng mà có thể phát sinh thêm chi phí khác như phí vận chuyển, phụ phí hàng cồng kềnh, thuế nhập khẩu (đối với đơn hàng giao từ nước ngoài có giá trị trên 1 triệu đồng).....", "")
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = text.lower()

    # if type == 'name':
    replace_list = ["hàng chính hãng", "chính hãng", "chi tiết", "phân phối", "hãng"]
    pattern = r'\b(?:{})\b'.format('|'.join(replace_list))
    text = re.sub(pattern, '', text)
    # else:
    #     text = text.replace("giá sản phẩm trên tiki đã bao gồm thuế theo luật hiện hành. bên cạnh đó, tuỳ vào loại sản phẩm, hình thức và địa chỉ giao hàng mà có thể phát sinh thêm chi phí khác như phí vận chuyển, phụ phí hàng cồng kềnh, thuế nhập khẩu (đối với đơn hàng giao từ nước ngoài có giá trị trên 1 triệu đồng).....", '')

    return text.strip()

def get_value(row_data, key, unpack_type=None):
    if key in row_data:
        value = row_data[key]
        if unpack_type:
            value = struct.unpack(unpack_type, value)[0]
        return value.decode('utf-8') if isinstance(value, bytes) else value
    return None

def read_product_data(key, row_data):
    product_id = struct.unpack('!i', key)[0]
    name = get_value(row_data, b'basic_info:name')
    # rating_average = get_value(row_data, b'basic_info:rating_average', '!f')
    # description = preprocess_text(get_value(row_data, b'basic_info:description'))
    seller = get_value(row_data, b'current_seller:name')
    category = get_value(row_data, b'categories:parent_name')

    return product_id, name, category, seller


def read_product_from_hbase(host, port, table_name):
    connection = happybase.Connection(host=host, port=port)
    table = connection.table(table_name.encode('utf-8'))
    rows = table.scan(limit=20)

    spark = SparkSession.builder.getOrCreate()

    records = []

    for key, data in rows:
        product_id, name, category, seller = read_product_data(key, data)
        records.append((product_id, name, category, seller))

    df = spark.createDataFrame(records, ["id", "name", "category", "seller"])

    connection.close()
    # spark.stop()
    return df

def train_recommendation_system(data_df, name_col, category_col, seller_col):
    # preprocess_desc_udf = udf(lambda text: preprocess_text(text, 'description'), StringType())

    # data_df = data_df.withColumn('rating_average', 
    #                              udf(lambda x: 'hài_lòng' if float(x) >= 4 else 'không_hài_lòng' if float(x) <= 2 else 'bình_thường', StringType())(col(rating_avg_col)))
    data_df = data_df.withColumn('seller', udf(lambda x: x.replace(' ', '_'), StringType())(col(seller_col)))

    preprocess_name_udf = udf(lambda text: preprocess_text(text), StringType())
    data_df = data_df.withColumn('metadata', concat(preprocess_name_udf(col(name_col)), lit(' '), col(category_col), lit(' ')))
    # data_df = data_df.withColumn('metadata',
    #                              concat(preprocess_name_udf(col(name_col)), lit(' '), preprocess_desc_udf(col(description_col)), lit(' '), col(category_col), lit(' '), col(seller_col)))
    
    tokenize_and_extract_words_udf = udf(tokenize_and_extract_words, ArrayType(StringType()))
    data_df = data_df.withColumn("words", tokenize_and_extract_words_udf(col('metadata')))
    
    # stopword_file = "/home/linhnq/graduation_project/vietnamese-stopwords-dash.txt"
    # with open(stopword_file, "r", encoding="utf-8") as file:
    #     stop_words = [line.strip() for line in file.readlines()]

    # remover = StopWordsRemover(inputCol='words', outputCol='filtered_words', stopWords=stop_words)
    
    # count_vectorizer = CountVectorizer(inputCol='words', outputCol='raw_features')
    # idf = IDF(inputCol='raw_features', outputCol='features')

    # pca = PCA(k=100, inputCol="features", outputCol="pca_features")

    # pipeline = Pipeline(stages=[remover, count_vectorizer, idf, pca])
    # pca_model = pipeline.fit(data_df)
    # pca_df = pca_model.transform(data_df)

    hashingTF = HashingTF(inputCol='words', outputCol='raw_features')
    idf = IDF(inputCol='raw_features', outputCol='features')

    pipeline = Pipeline(stages=[hashingTF, idf])
    tfidf_model = pipeline.fit(data_df)
    tfidf_df = tfidf_model.transform(data_df)
       
    assembler = VectorAssembler(inputCols=['features'], outputCol='features_vec')
    vec_df = assembler.transform(tfidf_df)
    vec_rdd = vec_df.select(col('id').alias('id1'), col('features_vec').alias('features_vec1')).rdd

    cosine_sim_rdd = vec_rdd.cartesian(vec_rdd).map(lambda x: (x[0]['id1'], x[1]['id1'], float(x[0]['features_vec1'].dot(x[1]['features_vec1']))))
    cosine_sim_df = cosine_sim_rdd.toDF(['id1', 'id2', 'cosine_sim'])

    def recommend_products(product_name, top=10):
        product_id = data_df.filter(col(name_col).contains(product_name)).select('id').first()[0]
        similar_products = cosine_sim_df.filter((col('id1') == product_id) & (col('id2') != product_id)) \
            .orderBy(col('cosine_sim').desc()).limit(top)
        recommended_products = similar_products.join(data_df, similar_products.id2 == data_df.id).select(name_col)
        return recommended_products

    return recommend_products


# data_df = read_product_from_hbase('localhost', 9090, 'product')
# data_df.show()

spark = SparkSession.builder.getOrCreate()

data_df = spark.read.csv('file:///home/linhnq/graduation_project/file_text/product50.csv', header=True, inferSchema=True)
# data_df = spark.createDataFrame(data_df)

recommend_products = train_recommendation_system(data_df, 'name', 'category', 'seller')

recommended_products = recommend_products('Thần Đồng Đất Việt 12 - Bạn Gái Bá Hộ', 10)
recommended_products.show()
