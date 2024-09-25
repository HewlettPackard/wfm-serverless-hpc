FROM python:3.9-slim-buster
 
# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN apt update --allow-unauthenticated && apt install python3-pip stress-ng -y
RUN apt install psmisc -y

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME

COPY requirements.txt ./
RUN pip3 install -r requirements.txt

RUN mkdir -p /data/
RUN chmod -R 777 /data
ENV PORT 8080

COPY . ./

# Expose the port the app runs on
#EXPOSE 5000
 
# Command to run the application with Gunicorn
#CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app"]
#CMD exec gunicorn --bind :$PORT --timeout 0 app:app 
CMD exec gunicorn --bind :$PORT --workers 960 --threads 1 --timeout 0 app:app