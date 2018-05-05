import sys
from cv2 import inRange

reload(sys)
from pyspark import SparkContext, SparkConf
import cv2

sys.setdefaultencoding('utf8')
import os
import re
import binascii

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

# from pyspark import SparkConf, SparkContext
# from hbase.ttypes import *
from hdfs import *
# import Hbase as hb
# import sys
import ast

# import cv2
# import os
# import re


# find_path = '/var/www/html/Spark_SQL'
find_path = '/var/www/html/database'


# faceCascade = cv2.CascadeClassifier('static/haarcascade/haarcascade_frontalcatface.xml')

class HbaseWrite():
    def __init__(self):
        self.tableName = 'database'
        self.transport = TSocket.TSocket('student62', 9090)
        self.transport = TTransport.TBufferedTransport(self.transport)
        self.transport.open()
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)

    def createTable(self):
        col1 = ColumnDescriptor(name="data:", maxVersions=1)
        # col2 = ColumnDescriptor(name="feature:", maxVersions=1)
        self.client.createTable(self.tableName, [col1])

    def write(self, PicPath, PicName):
        row = PicName.split('.')[0]
        _data = PicName.split('.')[1]
        PicData = open('%s/%s' % (PicPath, PicName), 'rb').read()
        PicData = binascii.hexlify(PicData)
        # TypeError: mutateRow() takes exactly 5 arguments (4 given)
        self.client.mutateRow(self.tableName, row, [Mutation(column='data:%s' % _data, value=PicData)])

    def write_feature(self, PicFeature, PicName):
        row = PicName.split('.')[0]
        _data = PicName.split('.')[1]
        # PicData = open('%s/%s' % (PicPath, PicName), 'rb').read()
        # PicData = binascii.hexlify(PicData)
        # TypeError: mutateRow() takes exactly 5 arguments (4 given)
        self.client.mutateRow(self.tableName, row, [Mutation(column='data:feature', value=PicFeature)])

    def read(self, tableName, PicName):
        row = PicName.split('.')[0]
        data_type = PicName.split('.')[1]
        get_data = self.client.get(tableName, row, 'data:%s' % data_type, {})[0]
        if get_data:
            return get_data.value
        else:
            return 'Error'


def find_img(_path):
    find_file = re.compile(r'^[0-9a-zA-Z\_]*.jpg$')
    find_walk = os.walk(_path)
    img = []

    for path, dirs, files in find_walk:
        for f in files:
            if find_file.search(f):
                path_name = path
                file_name = f
                catorigin = cv2.imread(path_name + '/' + file_name)
                # print path_name + '/' + file_name
                # print catorigin
                img_withname = []
                img_withname.append(f)
                img_withname.append(cut_cat_head(catorigin, f))
                img.append(img_withname)
            #                 WHB.write(path_name, file_name)
    return img


def cut_cat_head(img, f):
    tmp_img = img
    print f

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # print 1
    faceCascade = cv2.CascadeClassifier(
        '/var/www/html/FaceMatching_on_Spark/static/haarcascade/haarcascade_frontalcatface.xml')
    # print 11
    print faceCascade
    try:
        faces = faceCascade.detectMultiScale(
            gray,
            scaleFactor=1.10,
            minNeighbors=3,
            minSize=(5, 5),
            flags=cv2.CASCADE_SCALE_IMAGE
        )
        for (x, y, w, h) in faces:
            crop_img = img[y:y + h, x:x + w]
            res = cv2.resize(crop_img, (128, 128), interpolation=cv2.INTER_CUBIC)
        return res
    except:
        print 'fail'
        return tmp_img


def getdiff(name, img):
    Sidelength = 30
    img = cv2.resize(img, (Sidelength, Sidelength), interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    avglist = []
    for i in range(Sidelength):
        avg = sum(gray[i]) / len(gray[i])
        avglist.append(avg)
    WHB = HbaseWrite()
    WHB.write_feature(str(avglist), name)
    print "insert!"
    return avglist


img = find_img(find_path)
# print(data.glom().collect())
# print(img[0])
# imglength = len(img)
#
# rdd=[]
conf = SparkConf().setAppName("catcatcat").setMaster("yarn")
sc = SparkContext(conf=conf)

# for i in range(0,imglenth):
#     rdd.append(img[i])

# print(rdd.collect())
# data = sc.parallelize(img,200)
#     print(data.glom().collect())
# data1 = data.map(lambda x:[x[0],getdiff(x[0],x[1])]).collect()
# img = img[50:]
#    os.system("sync")
#    os.system("echo 3 > sudo /proc/sys/vm/drop_caches")
#     for i in range(0,100):
#         img.pop(0)
print("---------------------------------------------------")
for j in range(0, 403):
    rdd = []
    for i in range(0, 50):
        rdd.append(img[i])

    data = sc.parallelize(rdd, 30)
    # print(data.glom().collect())
    data1 = data.map(lambda x: [x[0], getdiff(x[0], x[1])]).collect()
    img = img[50:]
    # print j
    print j * 50 + i
    os.system("free -m")
# print(data1)
# WHB = HbaseWrite()
# for i in range(len(data1)):
#    print(data1[i][0])
#    print(data1[i][1])
#    WHB.write_feature('feature:'+str(data1[i][1]), data1[i][0])
print("===================================================")
