

запуск REDIS : docker run -d -p 6379:6379 redis
запуск CELERY: celery -A celery_app worker --loglevel=info

Inspect TASK:  celery -A celery_app inspect active

[![test_parser](https://github.com/Sapov/Parser_ya_maps_0/actions/workflows/python-app.yml/badge.svg)](https://github.com/Sapov/Parser_ya_maps_0/actions/workflows/python-app.yml)


