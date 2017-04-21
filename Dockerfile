FROM python:3.6-alpine


ADD model.py /
ADD tweetparser.py /
ADD requirements.txt /
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "./tweetparser.py"]