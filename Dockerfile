FROM python:2

RUN \
  apt-get update && apt-get install -y \
    git

ENV root /raildigitraffic2gtfsrt
RUN mkdir -p $root
WORKDIR $root

COPY requirements.txt /raildigitraffic2gtfsrt/requirements.txt
RUN pip install -r requirements.txt

ADD . $root
RUN pip install requests

ENV PORT=8080

CMD python app.py

