FROM alpine:3.6

WORKDIR /root

ENV LIBRDKAFKA=v0.9.5

RUN apk --update add --virtual build-dependencies gcc bash build-base git \
  && git clone https://github.com/edenhill/librdkafka.git \
  && cd /root/librdkafka \
  && git checkout ${LIBRDKAFKA} \
  && /root/librdkafka/configure \
  && make \
  && make install \
  && apk del build-dependencies \
  && rm -rf /root/librdkafka


# RUN echo http://mirror.yandex.ru/mirrors/alpine/v3.6/main > /etc/apk/repositories; \
#     echo http://mirror.yandex.ru/mirrors/alpine/v3.6/community >> /etc/apk/repositories

COPY requirements.txt message.avsc main.py /app-folder/

RUN apk --update add --virtual build-dependencies gcc bash python3-dev build-base git \
    && apk add --update --no-cache python3 py-pip \
    && pip3 install /app-folder/requirements.txt \
    && apk del build-dependencies

WORKDIR /app-folder

CMD python3 /app-folder/main.py
