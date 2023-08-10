from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyvi import ViTokenizer, ViPosTagger
from pyspark.ml.feature import StringIndexer, VectorIndexer, Word2Vec, Word2VecModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString
from pyspark.sql.functions import udf, concat, col, when, lit
from pyspark.sql.types import ArrayType, StringType
import re
import struct
import happybase
import csv
# from translate import Translator

def get_value(row_data, key, unpack_type=None):
    if key in row_data:
        value = row_data[key]
        if unpack_type:
            value = struct.unpack(unpack_type, value)[0]
        return value.decode('utf-8') if isinstance(value, bytes) else value
    return None

def read_rating_hbase():
    connection = happybase.Connection(host='localhost', port=9090)
    table_name = 'rating'
    table = connection.table(table_name.encode('utf-8'))
    rows = table.scan()
    
    data = []
    count = 1
    for row_key, row_data in rows:
        rating = get_value(row_data, b'review_info:rating', '!i')
        title = get_value(row_data, b'review_info:title')
        content = get_value(row_data, b'review_info:content')

        data.append((title, content, rating))
        print(count)
        count += 1

    connection.close()
#    df = spark.createDataFrame(data, ['title', 'content', 'rating'])
    return data

def read_from_csv():
    df = spark.read.csv("file:///home/linhnq/graduation_project/comment_predict/rating_data.csv", header=True)
    return df

def preprocess_text(text):
    if isinstance(text, str):
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\w\s]', ' ', text)
        text = text.lower()
        text = re.sub(r'(\w)\1+', r'\1', text)
        text = re.sub(r'\b(ok\b|ok\w+|oce|good|gud|nice)\b', 'hài lòng', text)
        return text.strip()
    else:
        return ''

def tokenize_and_extract_words(text):
    try:
        text = preprocess_text(text)
        words = ViTokenizer.tokenize(text).split(" ")
        return words
    except IndexError as e:
        print("IndexError:", e)
        return []
    
def determine_label(rating):
    if rating in ('1', '2'):
        return 'negative'
    elif rating == '3':
        return 'neutral'
    elif rating in ('4', '5'):
        return 'positive'
    else:
        return 'Unknown'

keyword_mapping = {
    'negative': ['hài lòng', 'bình thường', 'tốt', 'tuyệt vời', 'hay', 'ổn'],
    'positive': ['không hài lòng', 'k hài lòng', 'ko hài lòng', 'không tốt', 'k tốt','ko tốt', 'tạm được', 'tạm ổn', 'khá hài lòng', 'thất vọng', 'bình thường'],
    'neutral': ['hay', 'hài lòng', 'tốt']
}

def preprocess_title(title, rating):
    title = preprocess_text(title)
    label = determine_label(rating)

    if label in keyword_mapping:
        keywords = keyword_mapping[label]

        for keyword in keywords:
            if keyword in title:
                if label == 'negative':
                    title = 'không ' + title
                elif label == 'positive':
                    title = re.sub(r'\b(tạm|khá|thất vọng)\b', '', title)
                    title = title.replace('thất vọng', 'hài lòng')
                elif label == 'neutral':
                    title = title.replace('rất ', '')
                    title = 'bình thường'

                break

    return title

def preprocess_data(df):

    udf_determine_label = udf(determine_label, StringType())
    udf_preprocess_title = udf(preprocess_title, StringType())
    udf_preprocess_text = udf(preprocess_text, StringType())

    return df.withColumn('label', udf_determine_label(col('rating'))) \
                .withColumn('title', udf_preprocess_title(col('title'), col('rating'))) \
                .withColumn('content', concat(col('title'), lit(' '), udf_preprocess_text(col('content')))) \
                .select('content', 'label', 'rating') \
                .distinct()

def train_model(df):
    tokenize_and_extract_words_udf = udf(tokenize_and_extract_words, ArrayType(StringType()))
    df = df.withColumn("contentExtract", tokenize_and_extract_words_udf(df["content"]))

    # stopword_path = "/home/linhnq/graduation_project/file_text/vietnamese-stopwords-dash.txt"
    # with open(stopword_path, "r", encoding="utf-8") as file:
    #     stop_words = [line.strip() for line in file.readlines()]

    # remover = StopWordsRemover(inputCol='contentExtract', outputCol='filtered_words', stopWords=stop_words)
    # df = remover.transform(df)

    word2Vec = Word2Vec(vectorSize=100, seed=42, inputCol="contentExtract", outputCol="features")
    model_vec = word2Vec.fit(df)
    model_vec.write().overwrite().save("word2vec_model")
    data = model_vec.transform(df).select("label", "features")

    labelIndex = StringIndexer(inputCol="label", outputCol="indexLabel").fit(data)
    featureIndex = VectorIndexer(inputCol="features", outputCol="indexFeatures", maxCategories=5).fit(data)

    trainingData, testData = data.randomSplit([0.7, 0.3])

    num_trees = []
    accuracies = []
    f1_scores = []
    precisions = []
    for num_tree in range(20, 21):
        rf = RandomForestClassifier(labelCol="indexLabel", featuresCol="indexFeatures", numTrees=num_tree)

        labelConverter = IndexToString(inputCol="prediction", outputCol="predictLabel", labels=labelIndex.labels)

        pipeline = Pipeline(stages=[labelIndex, featureIndex, rf, labelConverter])

        model = pipeline.fit(trainingData)

        predictions = model.transform(testData)

        evaluator = MulticlassClassificationEvaluator(labelCol="indexLabel", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)

        evaluator_f1 = MulticlassClassificationEvaluator(labelCol="indexLabel", predictionCol="prediction", metricName="f1")
        f1_score = evaluator_f1.evaluate(predictions)

        evaluator_precision = MulticlassClassificationEvaluator(labelCol="indexLabel", predictionCol="prediction", metricName="weightedPrecision")
        precision = evaluator_precision.evaluate(predictions)

        num_trees.append(num_tree)
        accuracies.append(accuracy)
        f1_scores.append(f1_score)
        precisions.append(precision)

        print("Accuracy:", accuracy)
        print("F1-score:", f1_score)
        print("Weighted Precision:", precision)
        print("Test Error = %g" % (1.0 - accuracy))

        rfModel = model.stages[2]
        print(rfModel)

    import matplotlib.pyplot as plt
    plt.plot(num_trees, accuracies)
    plt.xlabel("Number of Trees")
    plt.ylabel("Accuracy")
    plt.title("Accuracy vs. Number of Trees")
    plt.show()


def predict_label(comment):
    #spark = SparkSession.builder.appName("NamedEntityRecognition").master('local[*]').getOrCreate()
    word2VecModel = Word2VecModel.load("word2vec_model")

    model = PipelineModel.load("classification_model")

    words = tokenize_and_extract_words(comment)

    df = spark.createDataFrame([(comment, words)], ["content", "contentExtract"])

    # stopword_file = "/content/drive/MyDrive/A_Project/vietnamese-stopwords-dash.txt"
    # with open(stopword_file, "r", encoding="utf-8") as file:
    #     stop_words = [line.strip() for line in file.readlines()]

    # remover = StopWordsRemover(inputCol='contentExtract', outputCol='filtered_words', stopWords=stop_words)
    # df = remover.transform(df)

    data = word2VecModel.transform(df).select("features")

    predictions = model.transform(data)

    predict_label = predictions.select("predictLabel").first()[0]

    #spark.stop()

    return predict_label

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HBaseSparkStreaming").master('local[*]').getOrCreate()

df = read_from_csv()
# df =read_rating_hbase()
df = preprocess_data(df)
df_final = df.filter(col('label') != 'Unknown')
df_final.groupBy("label").count().show()

train_model(df_final)

comment = "tất nhiên ai chẳng có phiền muộn trong lòng nhưng với nhóc nicolas thì mọi phiền muộn đều có thể giải quyết tuy rằng không phải theo 1 cách thông thường thỉnh thoảng khi buồn hoặc gặp rắc rối tôi lại lôi quyển này ra đọc và sau đó những vấn đề sẽ được nhìn lại dưới con mắt của nicolas tất nhiên là dễ dàng giải quyết hơn nhiều bạn hãy mua quyển này và thử trải nghiệm xem nói theo cách của nicolas thì hay kinh lên được"
predicted_label = predict_label(comment)
print(comment)
print("Predicted Label:", predicted_label)


# data = read_rating_hbase()

# Specify the output file path
# # output_path = "/home/linhnq/graduation_project/comment_predict/rating_data.csv"

# # Write the data to the CSV file
# with open(output_path, 'w', newline='') as csvfile:
#     csv_writer = csv.writer(csvfile)
    
#     # Write the header row
#     csv_writer.writerow(["title", "content", "rating"])
#     count = 1
#     # Write each data row
#     for row in data:
#         csv_writer.writerow(row)
#         print(count)
#         count += 1

