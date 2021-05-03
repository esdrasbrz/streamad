FROM python:3.7-slim

RUN mkdir /streamad
WORKDIR /streamad

COPY requirements.txt .
RUN pip install -r requirements.txt --extra-index-url https://test.pypi.org/simple/

COPY . /streamad

CMD python -m faust -A main worker -l info