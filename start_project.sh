sudo git reset --hard FETCH_HEAD
sudo git pull origin master
sudo chmod +x script/DetectCatFeatures.sh
sudo chmod +x script/CalculateSimilarity.sh
sudo python manage.py runserver 10.42.0.235:8000