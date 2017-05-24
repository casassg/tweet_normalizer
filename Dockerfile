FROM python:3.5-alpine

RUN apk add --no-cache gcc musl-dev
RUN sed -i -e 's/v3\.4/edge/g' /etc/apk/repositories  && \
    apk --no-cache add alpine-sdk librdkafka-dev

ADD requirements.txt /
RUN pip install appdirs --upgrade
RUN pip install -r requirements.txt
RUN apk del alpine-sdk && \
    rm -rf /root/cache/*

ADD model.py /
ADD tweetparser.py /

ENTRYPOINT ["python", "./tweetparser.py"]