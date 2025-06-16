# Проект 1 спринта

Добро пожаловать в репозиторий-шаблон Практикума для проекта 1 спринта. Цель проекта — создать базовое решение для предсказания стоимости квартир Яндекс Недвижимости.

Полное описание проекта хранится в уроке «Проект. Разработка пайплайнов подготовки данных и обучения модели» на учебной платформе.

Здесь укажите имя вашего бакета: s3-student-mle-20250529-e59a5780ac-freetrack

# Этап 1. Сбор данных

- DAG: ```/part1_airflow/dags/flats_buildings_join.py```
- Шаги пайплайна: ```/part1_airflow/plugins/steps/flats_buildings_join_steps.py```
- Telegram плагин: ```/part1_airflow/plugins/steps/messages.py```
- в БД создана таблица ```flats_buildings```

# Этап 2. Очистка данных

- DAG: ```/part1_airflow/dags/flats_buildings_clean.py```
- Шаги пайплайна: ```/part1_airflow/plugins/steps/flats_buildings_clean_steps.py```
- Telegram плагин: ```/part1_airflow/plugins/steps/messages.py```
- в БД создана таблица ```flats_buildings_clean```

# Этап 3. Создание DVC-пайплайна обучения модели
- dvc.yaml - ```/part2_dvc/dvc.yaml```
- params.yaml - ```/part2_dvc/params.yaml```
- Выгрузка данных - ```/part2_dvc/scripts/data.py```
- Обучение модели - ```/part2_dvc/scripts/fit.py```
- Оценка модели - ```/part2_dvc/scripts/evaluate.py```

# Запуск Airflow
## Запустить контейнер
```bash 
docker compose up --build
```
## Остановить
```bash 
docker compose down --volumes --remove-orphans
```
# Запуск обучения модели
1. Перейти в папку ```/part2_dvc```
2. выполнить команду 
```bash 
dvc repro
```