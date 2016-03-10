FROM python:2

ENV root /raildigitraffic2gtfsrt
RUN mkdir -p $root
WORKDIR $root

COPY requirements.txt /raildigitraffic2gtfsrt/requirements.txt
RUN pip install -r requirements.txt

ADD . $root

ENV PORT=8080
ENV ROUTER_ZIP_URL=http://beta.digitransit.fi/routing-data/v1/router-finland.zip

CMD python app.py

