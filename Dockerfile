FROM puckel/docker-airflow:latest
RUN pip install apache-airflow[kubernetes]