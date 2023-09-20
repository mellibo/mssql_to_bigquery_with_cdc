
FROM apache/beam_python3.10_sdk:2.50.0

RUN pip install --upgrade pip 
RUN apt update -y 

RUN apt install -y gcc unixodbc-dev  pyodbc

RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
RUN curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update -y
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18

RUN mkdir /var/df && mkdir /var/df/keys
ENV GOOGLE_APPLICATION_CREDENTIALS=/var/df/keys/gcpkey.json
COPY ./keys/key.json /var/df/keys/gcpkey.json
COPY ./sql_to_bigquery.py /var/df/

ENTRYPOINT [ "bash" ]