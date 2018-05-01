from pyspark import SparkConf, SparkContext
from hbase.ttypes import *
from hdfs import *
import Hbase as hb
import sys
import ast
import cv2
import os
import re

reload(sys)
sys.setdefaultencoding('utf8')

human_haar_cascade_path = "/var/www/html/FaceMatching_on_Spark/static/haarcascade/haarcascade_frontalface_default.xml"
result_photo = "/var/www/html/FaceMatching_on_Spark/static/image/cat_photo.png"
human_photo_path = '/var/www/html/FaceMatching_on_Spark/static/image/human_photo.png'
client = Client("http://student62:50070")
find_path = '/var/www/html/database'


# Detect features of human photo
def detect_human_feature():
    human_photo = cv2.imread(human_photo_path)
    gray = cv2.cvtColor(human_photo, cv2.COLOR_BGR2GRAY)
    face_cascade_human = cv2.CascadeClassifier(human_haar_cascade_path)
    faces = face_cascade_human.detectMultiScale(
        gray,
        scaleFactor=1.14,
        minNeighbors=5,
        minSize=(5, 5),
        flags=cv2.CASCADE_SCALE_IMAGE
    )

    for (x, y, w, h) in faces:
        crop_img = human_photo[y:y + h, x:x + w]
        human_feature = cv2.resize(crop_img, (128, 128), interpolation=cv2.INTER_CUBIC)

    return human_feature


# Get all the names of cats' photo
def find_img(_path):
    find_file = re.compile(r'^[0-9a-zA-Z\_]*.jpg$')
    find_walk = os.walk(_path)
    cat_photo_names = []

    for path, dirs, files in find_walk:
        for f in files:
            if find_file.search(f):
                cat_photo_names.append(f)

    return cat_photo_names


# Find cat features in Hbase
def find_feature_in_hbase(cat_img):
    cat_lists = []
    cat_features = []
    print "All the cat's photo amount = " + str(len(cat_img))
    for i in range(0, 9949):
        cat_feature = WHB.read_feature('database', cat_img[i])
        if cat_feature != 'Error':
            print "Photo No. = " + str(i) + "    Photo Name = " + str(cat_img[i])
            cat_features.append([cat_img[i], ast.literal_eval(cat_feature)])
            cat_lists.append(cat_features[i])
            print "Find features in Hbase!"
        else:
            print "Can not find the features in Hbase! The photo name is " + str(cat_img[i])
    return cat_lists


def get_diff(img):
    side_length = 30
    img = cv2.resize(img, (side_length, side_length), interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    avg_list = []

    for i in range(side_length):
        avg = sum(gray[i]) / len(gray[i])
        avg_list.append(avg)

    return avg_list


# Calculate the variance
def get_ss(lists):
    ss = 0
    avg = sum(lists) / len(lists)
    for l in lists:
        ss += (l - avg) * (l - avg) / len(lists)

    return ss


def get_avg(f1, f2):
    diff_list = []
    for i in range(30):
        avg1 = f1[i] - f2[i]
        diff_list.append(avg1)
    return diff_list


res = detect_human_feature()
diff_human = get_diff(res)
cat_img = find_img(find_path)
WHB = hb.HbaseWrite()
cat_lists = find_feature_in_hbase(cat_img)

# Run on spark
conf = SparkConf().setAppName("FindCat").setMaster("yarn")

sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
data = sc.parallelize(cat_lists, 8)

map_res_1 = data.map(lambda x: [x[0], x[1]])
map_res_2 = map_res_1.map(lambda x: [x[0], get_avg(x[1], diff_human)])
map_res_3 = map_res_2.map(lambda x: [x[0], get_ss(x[1])])
map_res_4 = map_res_3.sortBy(lambda x: x[1], ascending=True).take(1)
print "Find the most similar cat's photo! : " + map_res_4[0][0]
cat_photo_path = "/var/www/html/database/Cat/" + map_res_4[0][0]
os.system("cp %s %s" % (cat_photo_path, result_photo))
