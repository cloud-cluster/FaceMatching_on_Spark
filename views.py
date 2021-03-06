from django.shortcuts import render
from django.http import HttpResponse
from hdfs import *
import json
import cv2
import sys
import os

reload(sys)
sys.setdefaultencoding('utf8')

human_photo_path = "/FaceMatching_on_Spark/Results/human_photo.png"
cat_photo_path = "static/image/cat_photo.png"
human_haar_cascade_path = "static/haarcascade/haarcascade_frontalface_default.xml"

client = Client("http://student62:50070")


# function of detecting human face
# parameter specification
#   human_path:         path to the human photo which is newly uploaded
# return specification
#   return 1: success
#   return 0: fail to detect human face
def detect_human(human_path):
    img = cv2.imread(human_path)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    face_cascade_human = cv2.CascadeClassifier(human_haar_cascade_path)
    faces = face_cascade_human.detectMultiScale(
        gray,
        scaleFactor=1.14,
        minNeighbors=5,
        minSize=(5, 5),
        flags=cv2.CASCADE_SCALE_IMAGE
    )

    if str(faces) == "()":
        return 0
    else:
        return 1


def home(request):
    return render(request, 'home.html')


def upload(request):
    if request.method == "POST":
        ret = {'status': 'fail', 'data': {'human_photo': None, 'cat_photo': None}, 'error': None, 'Access-Control-Allow-Origin': '*'}

        os.system("rm -rf /var/www/html/FaceMatching_on_Spark/static/image/cat_photo.png")
        os.system("rm -rf /var/www/html/FaceMatching_on_Spark/static/image/human_photo.png")
        pic_file = request.FILES.get("photo")       # get photo
        file_path = os.path.join("static/image", "human_photo.png")
        f = open(file_path, mode="wb")
        for chunk in pic_file.chunks():
            f.write(chunk)
        f.close()
        if detect_human(file_path) == 0:
            ret['status'] = 'fail'
            ret['error'] = 0
        else:
            client.upload(human_photo_path, file_path, overwrite=True)

            # os.system("/opt/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master=yarn --driver-memory 7168m --executor-memory 4G /var/www/html/FaceMatching_on_Spark/calculate_similarity.py")

            # result = 0

            # if result == 0:
            #     ret['status'] = 'success'
            #     ret['data']['human_photo'] = file_path
            #     ret['data']['cat_photo'] = cat_photo_path
            # else:
            #     ret['error'] = 1

            ret['status'] = 'success'
            ret['data']['human_photo'] = file_path
            ret['data']['cat_photo'] = cat_photo_path
            # ret["Access-Control-Allow-Origin"] = "*"
        return HttpResponse(json.dumps(ret), content_type='application/json')


def test(request):
    os.system("/opt/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master=yarn /var/www/html/FaceMatching_on_Spark/calculate_similarity.py")
    # rs.run_spark()
