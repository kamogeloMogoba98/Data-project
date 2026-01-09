# Use a Debian-based JDK image for M1/M2 Mac compatibility
FROM eclipse-temurin:21-jre-jammy

# Set Metabase version and directory
ENV MB_PLUGINS_DIR=/plugins
WORKDIR /app

# Step 1: Use the Ubuntu-based Metabase image (essential for DuckDB/glibc)
FROM metabase/metabase:latest-ubuntu

# Step 2: Set environment variables
# MB_PLUGINS_DIR tells Metabase exactly where to scan for .jar drivers
ENV MB_PLUGINS_DIR=/plugins

# Step 3: Switch to root to handle file system setup
USER root

# Install curl (used for your setup scripts later)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create the plugins directory
RUN mkdir -p /plugins

# Step 4: Add the DuckDB driver directly into the image
# This ensures the driver is baked in, even if your volume mount fails
ADD https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/1.4.3.0/duckdb.metabase-driver.jar /plugins/duckdb.metabase-driver.jar

# Set permissions so the 'metabase' user can read the driver
RUN chmod 744 /plugins/duckdb.metabase-driver.jar && \
    chown -R metabase:metabase /plugins

# Step 5: Switch back to the standard metabase user for safety
USER metabase

# Expose the default port
EXPOSE 3000

# Step 6: Use the standard Metabase run script
# This is better than 'java -jar' because it handles setup tasks automatically
ENTRYPOINT ["/app/run_metabase.sh"]