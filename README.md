# Проект 1 спринта

Добро пожаловать в репозиторий-шаблон Практикума для проекта 1 спринта. Цель проекта — создать базовое решение для предсказания стоимости квартир Яндекс Недвижимости.

Полное описание проекта хранится в уроке «Проект. Разработка пайплайнов подготовки данных и обучения модели» на учебной платформе.

Здесь укажите имя вашего бакета: s3-student-mle-20250529-e59a5780ac-freetrack

# Этап 1. Сбор данных

- DAG: ```/part1_airflow/dags/flats_pipeline.py```
- Шаги пайплайна: ```/part1_airflow/plugins/steps/flats_pipeline_steps.py```
- Telegram плагин: ```/part1_airflow/plugins/steps/messages.py```

# Запуск Airflow
## Запустить контейнер
```bash 
docker compose up --build
```
## Остановить
```bash 
docker compose down --volumes --remove-orphans
```
