from pyspark.sql import SparkSession
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

def preprocess_text(text):
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = text.lower()
    return text.strip()

def tokenize_and_extract_words(text):
    try:
        text = preprocess_text(text)
        words = ViTokenizer.tokenize(text).split(" ")
        return words
    except IndexError as e:
        print("IndexError:", e)
        return []
    
def get_value(row_data, key, unpack_type=None):
    if key in row_data:
        value = row_data[key]
        if unpack_type:
            value = struct.unpack(unpack_type, value)[0]
        return value.decode('utf-8') if isinstance(value, bytes) else value
    return None

def read_review_from_hbase():
    connection = happybase.Connection(host='localhost', port=9090)
    table_name = 'rating'
    table = connection.table(table_name.encode('utf-8'))
    data = []
    rows = table.scan()  

    for row_key, row_data in rows:
        rating = get_value(row_data, b'review_info:rating', '!i')
        content = get_value(row_data, b'review_info:content')

        if content is not None and content.strip() != '':
            content = preprocess_text(content)

            if rating in [1, 2]:
                label = 'negative'
            elif rating == 3:
                label = 'neutral'
            elif rating in [4, 5]:
                label = 'positive'
            else:
                label = 'Unknown'

            data.append((content, label))

    connection.close()

    df = spark.createDataFrame(data, ["content", "label"])
    connection.close()
    return df

def determine_label(rating):
    if rating in ('1', '2'):
        return 'negative'
    elif rating == '3':
        return 'neutral'
    elif rating in ('4', '5'):
        return 'positive'
    else:
        return 'Unknown'
    
def read_from_csv():
    # df = df.fillna({'content': 'Hài lòng'})
    # id,title,content,customer_id,rating
    df = spark.read.csv("file:///home/linhnq/graduation_project/file_text/rating_temp.csv", header=True)

    udf_determine_label = udf(determine_label, StringType())
    new_df = df.withColumn('label', udf_determine_label(col('rating'))) \
                .withColumn('content', concat(col('title'), lit(' '), col('content'))) \
                .select('content', 'label')\
                .distinct()
    
    return new_df

def train_model(df):
    tokenize_and_extract_words_udf = udf(tokenize_and_extract_words, ArrayType(StringType()))
    df = df.withColumn("contentExtract", tokenize_and_extract_words_udf(df["content"]))

    # stopword_file = "/home/linhnq/graduation_project/vietnamese-stopwords-dash.txt"
    # with open(stopword_file, "r", encoding="utf-8") as file:
    #     stop_words = [line.strip() for line in file.readlines()]

    # remover = StopWordsRemover(inputCol='contentExtract', outputCol='filtered_words', stopWords=stop_words)
    # df = remover.transform(df)

    word2Vec = Word2Vec(vectorSize=100, seed=42, inputCol="filtered_words", outputCol="features")
    model_vec = word2Vec.fit(df)
    model_vec.write().overwrite().save("word2vec_model")
    data = model_vec.transform(df).select("label", "features")

    labelIndex = StringIndexer(inputCol="label", outputCol="indexLabel").fit(data)
    featureIndex = VectorIndexer(inputCol="features", outputCol="indexFeatures", maxCategories=5).fit(data)

    trainingData, testData = data.randomSplit([0.7, 0.3])
    
    num_trees = []
    accuracies = []
    for num_tree in range(10, 11):
        rf = RandomForestClassifier(labelCol="indexLabel", featuresCol="indexFeatures", numTrees=num_tree)

        labelConverter = IndexToString(inputCol="prediction", outputCol="predictLabel", labels=labelIndex.labels)

        pipeline = Pipeline(stages=[labelIndex, featureIndex, rf, labelConverter])

        model = pipeline.fit(trainingData)

        predictions = model.transform(testData)

        evaluator = MulticlassClassificationEvaluator(labelCol="indexLabel", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)

        num_trees.append(num_tree)
        accuracies.append(accuracy)
        print("Accuracy:", accuracy)
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
    word2VecModel = Word2VecModel.load("word2vec_model")
    model = PipelineModel.load("classification_model")

    words = tokenize_and_extract_words(comment)
    df = spark.createDataFrame([(comment, words)], ["content", "contentExtract"])

    # stopword_file = "/home/linhnq/graduation_project/vietnamese-stopwords-dash.txt"
    # with open(stopword_file, "r", encoding="utf-8") as file:
    #     stop_words = [line.strip() for line in file.readlines()]

    # remover = StopWordsRemover(inputCol='contentExtract', outputCol='filtered_words', stopWords=stop_words)
    # df = remover.transform(df)


    data = word2VecModel.transform(df).select("features")

    predictions = model.transform(data)

    predict_label = predictions.select("predictLabel").first()[0]


    return predict_label

spark = SparkSession.builder.appName("SparkPredictLabel").getOrCreate()

df = read_from_csv()
train_model(df)

comment = "hài lòng về sản phẩm, rất tuyệt"
predicted_label = predict_label(comment)
print("Predicted Label:", predicted_label)
spark.stop()
