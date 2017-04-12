FROM python:3.6-alpine


ADD streaming.py /
ADD requirements.txt /
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "./eventconsumer.py"]