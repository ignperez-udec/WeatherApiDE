#Python
FROM python:3.12-slim

#Work folder of container
WORKDIR /WeatherApiDE

# Install PostgreSQL client (for pg_isready)
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

# Install dbt (choose your adapter)
RUN pip install --no-cache-dir dbt-postgres

#Copy dependencies
COPY python/requirements.txt python/requirements.txt
RUN pip install --no-cache-dir -r python/requirements.txt

#Copy and run entrypoint script
COPY python/init.sh python/init.sh
RUN chmod +x python/init.sh

#Copy source and data files
COPY src src
COPY data data

#Define entrypoint
ENTRYPOINT ["bash","/WeatherApiDE/python/init.sh"]
