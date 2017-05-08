FROM python:3.6-alpine
RUN apk add --no-cache gcc musl-dev

ADD requirements.txt /
RUN pip install -r requirements.txt
ADD model.py /
ADD tweetparser.py /

ENTRYPOINT ["python", "./tweetparser.py"]