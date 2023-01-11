FROM bde2020/spark-submit:3.0.0-hadoop3.2

# Add build dependencies for c-libraries (important for building numpy and other sci-libs)
RUN apk --no-cache add --virtual build-deps musl-dev linux-headers g++ gcc python3-dev

# Copy the requirements.txt first, for separate dependency resolving and downloading
COPY requirements.txt /app/
RUN cd /app \
    && pip3 install --upgrade pip \
    && pip3 install -r requirements.txt

# Copy the source code
COPY ./src /app/src

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/src/main.py
ENV SPARK_APPLICATION_ARGS "--job train"
