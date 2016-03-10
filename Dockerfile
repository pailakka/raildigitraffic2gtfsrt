FROM python:2

ENV root /raildigitraffic2gtfsrt
RUN mkdir -p $root
WORKDIR $root

COPY requirements.txt /raildigitraffic2gtfsrt/requirements.txt
RUN pip install -r requirements.txt

ADD . $root

ENV PORT=8080

CMD python app.py

