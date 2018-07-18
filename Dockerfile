FROM python:3.7
RUN mkdir /code
WORKDIR /code
ADD ./requirements.txt /code/requirements.txt
RUN pip install -r requirements.txt
ADD . /code
CMD ["python", "pipeline.py", "--source", "https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-02-07.csv", "--sink", "mysql://root:password@mysql/external/npi"]
