# Образ который расширяем
FROM apache/airflow:2.7.3-python3.10
# копируем файл в целевую директорию
COPY requirements.txt ./tmp/requirements.txt
# Устанавливаем/Обновляем pip на всякий случай
RUN pip install -U pip
# Устанавливаем все Python-пакеты, перечисленные в файле requirements.txt
RUN pip install -r ./tmp/requirements.txt