# Use a Debian-based JDK image for M1/M2 Mac compatibility
FROM eclipse-temurin:21-jre-jammy

# Set Metabase version and directory
ENV MB_PLUGINS_DIR=/plugins
WORKDIR /app

# Install dependencies (curl for downloading)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Download Metabase JAR directly (v0.51.5 is the current stable)
ADD https://downloads.metabase.com/v0.51.5/metabase.jar /app/metabase.jar

# Create plugins directory and download DuckDB driver
RUN mkdir -p /plugins
ADD https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/1.4.3.0/duckdb.metabase-driver.jar /plugins/duckdb.metabase-driver.jar
RUN chmod 744 /plugins/duckdb.metabase-driver.jar

# Expose Metabase port
EXPOSE 3000

# Start Metabase
ENTRYPOINT ["java", "-jar", "/app/metabase.jar"]