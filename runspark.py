from pyspark import SparkConf, SparkContext
import os


def run_spark():
    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf=conf)
    os.system("/opt/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master=yarn --driver-memory 7168m --executor-memory 4G /var/www/html/FaceMatching_on_Spark/calculate_similarity.py")

    return 0