FROM python:3.6-alpine

ADD requirements.txt /
RUN pip install -r requirements.txt
ADD model.py /
ADD tweetparser.py /

ENTRYPOINT ["python", "./tweetparser.py"]