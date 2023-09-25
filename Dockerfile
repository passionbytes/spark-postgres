FROM bitnami/spark
USER root
RUN install_packages curl
USER 1001
RUN curl https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -o /opt/bitnami/spark/jars/postgresql-42.2.18.jar
