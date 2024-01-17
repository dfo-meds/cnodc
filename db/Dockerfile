FROM postgis/postgis:15-3.4
ENV POSTGRES_DB nodb
COPY ./*.sql /docker-entrypoint-initdb.d/
EXPOSE 5432
