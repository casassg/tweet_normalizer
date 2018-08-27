all: eventparser
push: push-eventparser
.PHONY: push push-eventparser eventparser

# To bump the Zeppelin version, bump the version in
# zeppelin/Dockerfile and bump this tag and reset to v1.
TAG = 1.2.8

eventparser:
	docker build -t projectepic/tweet-cassandra .
	docker tag projectepic/tweet-cassandra projectepic/tweet-cassandra:$(TAG)

push-eventparser: eventparser
	docker push projectepic/tweet-cassandra
	docker push projectepic/tweet-cassandra:$(TAG)

clean:
	docker rmi projectepic/tweet-cassandra:$(TAG) || :
	docker rmi projectepic/tweet-cassandra || :
