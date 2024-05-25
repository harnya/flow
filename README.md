## Start celery worker 
celery -A tasks worker -B --loglevel=info -Ofair
