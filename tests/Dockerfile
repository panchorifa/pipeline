FROM python:3.7
RUN mkdir /code
WORKDIR /code
ADD ./requirements.txt /code/requirements.txt
ADD ./requirements_dev.txt /code/requirements_dev.txt
RUN pip install -r requirements.txt requirements_dev.txt
ADD . /code
