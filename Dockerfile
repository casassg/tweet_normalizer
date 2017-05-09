FROM python:2.7-alpine
RUN apk add --no-cache gcc musl-dev

ADD requirements.txt /
RUN pip install appdirs --upgrade
RUN pip install -r requirements.txt
ADD model.py /
ADD tweetparser.py /

ENTRYPOINT ["python", "./tweetparser.py"]