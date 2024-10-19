
# Use a base image that includes Chrome
FROM selenium/standalone-chrome:latest

# Install necessary dependencies
USER root

# Install wget and unzip to download ChromeDriver
RUN apt-get update && \
    apt-get install -y wget unzip && \
    apt-get clean

# Set the ChromeDriver URL
ENV CHROMEDRIVER_URL="https://storage.googleapis.com/chrome-for-testing-public/129.0.6668.89/linux64/chromedriver-linux64.zip"

# Install ChromeDriver
RUN wget -N $CHROMEDRIVER_URL && \
    unzip chromedriver-linux64.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/chromedriver-linux64/chromedriver && \
    mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    rm -r /usr/local/bin/chromedriver-linux64 && \
    rm chromedriver-linux64.zip

FROM apache/airflow:2.10.2

USER root

# Install bash
RUN apt-get update && apt-get install -y bash && rm -rf /var/lib/apt/lists/*

# Switch back to the default user
USER airflow

