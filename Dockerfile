FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
WORKDIR /app
COPY . /app
USER root
RUN python3 -m pip install pyspark==3.1.2
RUN python3 -m pip install boto3
CMD python3 source/ingestion.py

