FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
	&& apt-get install -y --no-install-recommends libnbd-bin python3-libnbd \
	&& rm -rf /var/lib/apt/lists/*
