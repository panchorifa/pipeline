FROM python:3.7
RUN mkdir /code
WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install -r requirements.txt
COPY . /code
CMD ["python", "pipeline.py", "--source-url", "https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-02-07.csv", "--sink-user", "root", "--sink-password", "password", "--sink-host", "mysql", "--sink-database", "external", "--sink-table", "npi"]
