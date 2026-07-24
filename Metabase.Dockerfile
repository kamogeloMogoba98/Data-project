FROM metabase/metabase:latest

ENV MB_PLUGINS_DIR=/plugins

USER root

RUN mkdir -p /plugins

ADD https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/1.4.3.0/duckdb.metabase-driver.jar /plugins/duckdb.metabase-driver.jar

RUN chmod 744 /plugins/duckdb.metabase-driver.jar

EXPOSE 3000

ENTRYPOINT ["/app/run_metabase.sh"]