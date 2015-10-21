import gtfs_realtime_pb2
from flask import Flask
import time
import os
import requests
import pprint
import datetime
import threading
from dateutil.tz import tzlocal,tzutc
import copy
import sys
import math
import shutil

import zipfile
import unicodecsv

OTP_BASE = 'http://digitransit.fi/otp'
OTP_ROUTER = 'finland'

DEBUG = False

def getCompTime(eventrow):
    if 'actualTime' in eventrow:
        return eventrow['actualTime']
    if 'liveEstimateTime' in eventrow:
        return eventrow['liveEstimateTime']
    if 'scheduledTime' in eventrow:
        return eventrow['scheduledTime']

class railDigitrafficClient(threading.Thread):
    def __init__(self,category_filters=None,type_filters=None,keep_timetable_rows=False):
        super(railDigitrafficClient,self).__init__()
        self.daemon = True
        self.running = True
        self.trains = {}
        self.lines = {}
        self.latest_version = None
        self.data_loaded = False

        self.utc = tzutc()
        self.local = tzlocal()

        self.category_filters = category_filters
        self.type_filters = type_filters
        self.keep_timetable_rows = keep_timetable_rows

    def convertTimetable(self,eventrow):
        if 'scheduledTime' in eventrow:
            eventrow['scheduledTime'] = datetime.datetime.strptime(eventrow['scheduledTime'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=self.utc).astimezone(self.local)
        if 'actualTime' in eventrow:
            eventrow['actualTime'] = datetime.datetime.strptime(eventrow['actualTime'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=self.utc).astimezone(self.local)
        if 'liveEstimateTime' in eventrow:
            eventrow['liveEstimateTime'] = datetime.datetime.strptime(eventrow['liveEstimateTime'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=self.utc).astimezone(self.local)

        return eventrow

    def getTrainDataCopy(self):
        while not self.data_loaded:
            time.sleep(2)
        return copy.copy(self.trains)

    def run(self):
        while self.running:
            self.data_loaded = False
            params = None
            if self.latest_version != None:
                params = {'version':self.latest_version}
            st = time.time()
            try:
                r = requests.get('http://rata.digitraffic.fi/api/v1/live-trains',params=params)
                traindata = r.json()
                r.close()
            except:
                print 'live-trains request failed'
                time.sleep(20)
                continue
            #print 'data',time.time()-st
            st = time.time()
            del r
            tn = tn2 = 0
            for t in traindata:
                tn+=1
                if self.category_filters and not t['trainCategory'] in self.category_filters:
                    continue

                if self.type_filters and not t['trainType'] in self.type_filters:
                    continue

                # Juna saapunut maaranpaahan
                if 'actualTime' in t['timeTableRows'][-1]:
                    continue

                # Juna ei ole viel lahtenut -> ei ennusteita
                if not t['runningCurrently'] and not t['cancelled']:
                    continue

                if not 'actualTime' in t['timeTableRows'][0]:
                    continue

                t['timeTableRows'] = map(self.convertTimetable,t['timeTableRows'])

                t['first'] = t['timeTableRows'][0]
                t['last'] = t['timeTableRows'][-1]

                if not self.keep_timetable_rows:
                    del t['timeTableRows']

                if not self.latest_version or t['version'] > self.latest_version:
                    self.latest_version = t['version']

                self.trains[t['trainNumber']] = t
                tn2 += 1

            del traindata


            #print 'saving',time.time()-st
            #print time.ctime(),tn2,tn,len(self.trains)

            tns = self.trains.keys()
            now = datetime.datetime.utcnow().replace(tzinfo=self.utc)
            past_limit = now-datetime.timedelta(hours=4)
            future_limit = now+datetime.timedelta(hours=4)
            for tn in tns:
                if getCompTime(self.trains[tn]['first']) > future_limit:
                    #print 'future',tn
                    del self.trains[tn]
                    continue
                if getCompTime(self.trains[tn]['last']) < past_limit:
                    #print 'past',tn
                    del self.trains[tn]
                    continue
            '''
            self.lines = {}
            for tn in self.trains:
                if not 'commuterLineID' in self.trains[tn]:
                    continue
                clid = self.trains[tn]['commuterLineID']
                if clid not in self.lines:
                    self.lines[clid] = []
                self.lines[clid].append(self.trains[tn])
            '''
            self.data_loaded = True
            time.sleep(10)
        print 'Done!'


class stop2stationResolver:
    dt_stations = {}
    name2station = {}
    stop2station = {}

    def __init__(self):
        if len(self.dt_stations) == 0:
            self.loadDTStations()

    def loadDTStations(self):

        r = requests.get('http://rata.digitraffic.fi/api/v1/metadata/station')
        stations = r.json()


        for st in stations:
            #Jostain syysta tallainenkin loytyy...
            if st['stationShortCode'].endswith('_POIS'):
                continue
            self.dt_stations[st['stationShortCode']] = (st['stationShortCode'],st['stationName'],st['latitude'],st['longitude'])

            assert st['stationName'].lower() not in self.name2station
            self.name2station[st['stationName'].lower()] = st['stationShortCode']

    def getStationForStop(self,stop):
        gtfsid = stop['stop_id']
        if gtfsid in self.stop2station:
            return self.stop2station[gtfsid]

        station = None
        name_lwr = stop['stop_name'].lower()
        if name_lwr in self.name2station:
            self.stop2station[gtfsid] = self.name2station[name_lwr]
            return self.name2station[name_lwr]


        nearest = None
        stop_lat = float(stop['stop_lat'])
        stop_lon = float(stop['stop_lon'])

        for s in self.dt_stations:
            st = self.dt_stations[s]
            reldist = math.hypot(
                (st[3]-stop_lon)*math.cos(math.radians((st[2]+stop_lat)/2.0)),
                st[2]-stop_lat
                )

            if nearest == None or reldist < nearest[0]:
                nearest = (reldist,st)

        if nearest[0] > 0.05:
            print 'FINDING STATION MATCH FAILED',stop
            return None

        self.stop2station[gtfsid] = nearest[1][0]
        return nearest[1][0]

def loadGTFSRailTripData(gtfs_package = 'vr.zip'):
    st = time.time()
    wanted_stopids = set([])
    wanted_routeids = set([])
    wanted_tripids = set([])

    routes = {}
    trips = {}
    stops = {}

    zf = zipfile.ZipFile(gtfs_package)

    with zf.open('routes.txt','r') as f:
        csvf = unicodecsv.reader(f, delimiter=',', quotechar='"')
        headers = None
        for l in csvf:
            if headers == None:
                headers = l
                continue
            l = dict(zip(headers,l))
            if l['route_type'] == '2' or int(l['route_type'])/100 == 1:
                wanted_routeids.add(l['route_id'])
                routes[l['route_id']] = l
                routes[l['route_id']]['trips'] = []

    print gtfs_package,len(routes),'routes loaded'

    with zf.open('trips.txt','r') as f:
        csvf = unicodecsv.reader(f, delimiter=',', quotechar='"')
        headers = None
        for l in csvf:
            if headers == None:
                headers = l
                continue
            l = dict(zip(headers,l))
            if not l['route_id'] in wanted_routeids:
                continue

            wanted_tripids.add(l['trip_id'])
            trips[l['trip_id']] = l
            trips[l['trip_id']]['stoptimes'] = []
            routes[l['route_id']]['trips'].append(l['trip_id'])

    print gtfs_package,len(trips),'trips loaded'

    z = 0
    with zf.open('stop_times.txt','r') as f:
        csvf = unicodecsv.reader(f, delimiter=',', quotechar='"')
        headers = None
        for l in csvf:
            if headers == None:
                headers = l
                continue
            l = dict(zip(headers,l))
            if not l['trip_id'] in wanted_tripids:
                continue
            trips[l['trip_id']]['stoptimes'].append(l)
            wanted_stopids.add(l['stop_id'])
            z+=1
    print gtfs_package,z,'stoptimes loaded'

    with zf.open('stops.txt','r') as f:
        csvf = unicodecsv.reader(f, delimiter=',', quotechar='"')
        headers = None
        for l in csvf:
            if headers == None:
                headers = l
                continue
            l = dict(zip(headers,l))
            if not l['stop_id'] in wanted_stopids:
                continue

            stops[l['stop_id']] = l

    print gtfs_package,len(stops),'stops loaded'

    zf.close()
    print gtfs_package,'reading done. Took:',time.time()-st
    return routes,trips,stops

def gtfstime2timedelta(gtfstime):
    t = map(int,gtfstime.split(':'))

    return datetime.timedelta(seconds=t[0]*3600+t[1]*60+t[2])

class railGTFSRTProvider:
    def __init__(self,train_dt=None,gtfs_source='vr.zip'):
        self.s2sr = stop2stationResolver()
        self.train_dt = train_dt
        self.stop2station = {}
        self.entid = 1
        self.routes,self.trips,self.stops = loadGTFSRailTripData(gtfs_source)

        self.handleGTFSRouteData()


    def handleGTFSRouteData(self):
        self.longdistance = {}
        self.commuter = {}
        otptrains = []
        today = datetime.datetime.utcnow().replace(hour=0,minute=0,second=0,microsecond=0)
        for routeid in self.routes:
            route = self.routes[routeid]
            trips = {}
            for tripid in route['trips']:
                t = self.trips[tripid]

                depart = t['stoptimes'][0]['departure_time']
                depart_stop = t['stoptimes'][0]['stop_id']

                depart_station = self.s2sr.getStationForStop(self.stops[depart_stop])

                tk = (depart_station,(today+gtfstime2timedelta(depart)).time())

                if not tk in trips:
                    trips[tk] = []

                trips[tk].append((tripid,[(st['arrival_time'],st['departure_time'],self.s2sr.getStationForStop(self.stops[st['stop_id']]),st['stop_id']) for st in t['stoptimes']]))

            if route['route_short_name'].isdigit():
                if int(route['route_short_name']) not in self.longdistance:
                    self.longdistance[int(route['route_short_name'])] = []
                self.longdistance[int(route['route_short_name'])].append((route['route_id'],trips))
            else:
                if route['route_short_name'] not in self.commuter:
                    self.commuter[route['route_short_name']] = []
                self.commuter[route['route_short_name']].append((route['route_id'],trips))



    def matchTrainsAndCreateMessage(self,msg,train,otp_trips):
        today = datetime.datetime.now().replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=tzlocal())
        station_times = {}

        #ix 1
        for tr in train['timeTableRows']:
            if not tr['trainStopping']:
                continue

            if not tr['commercialStop']:
                continue

            station_times[(tr['stationShortCode'],tr['type'],tr['scheduledTime'].time())] = tr



        tk = (train['first']['stationShortCode'],train['first']['scheduledTime'].time())
        for r in otp_trips:
            routeid = r[0]
            for t in r[1]:
                if t != tk:
                    continue
                for tripid,stops in r[1][t]:
                    ix = 1


                    stus = []
                    skipped = False
                    for arr,dep,station,stopid in stops:
                        arr =(today+gtfstime2timedelta(arr)).time()
                        dep =(today+gtfstime2timedelta(dep)).time()

                        dt_arr = dt_dep = None
                        lkp = station
                        if (lkp,'ARRIVAL',arr) in station_times:
                            dt_arr = station_times[(lkp,'ARRIVAL',arr)]
                        elif (lkp,'DEPARTURE',arr) in station_times:
                            dt_arr = station_times[(lkp,'DEPARTURE',arr)]

                        if (lkp,'DEPARTURE',dep) in station_times:
                            dt_dep = station_times[(lkp,'DEPARTURE',dep)]

                        if ix == 1 and dt_arr == None:
                            dt_arr = dt_dep

                        if dt_dep == None and ix == len(stops):
                            dt_dep = dt_arr


                        if dt_arr and dt_dep:
                            stus.append((ix,stopid,dt_arr,dt_dep))
                        else:
                            skipped = True
                            break

                        ix+=1

                    if skipped:
                        #print routeid,tripid,train['trainNumber'],train['commuterLineID'] if 'commuterLineID' in train else '','invalid match'
                        continue

                    ent = msg.entity.add()
                    ent.id = str(self.entid)
                    self.entid+=1
                    if DEBUG:
                        tripid += '-' + str(train['trainNumber']) + ('-%s' % train['commuterLineID'] if 'commuterLineID' in train else '')
                    ent.trip_update.trip.trip_id = tripid
                    ent.trip_update.trip.route_id = routeid
                    ent.trip_update.trip.schedule_relationship = ent.trip_update.trip.SCHEDULED
                    ent.trip_update.timestamp = int(time.time())


                    for ix,stopid,dt_arr,dt_dep in stus:
                            stu = ent.trip_update.stop_time_update.add()
                            stu.stop_sequence = ix
                            stu.stop_id = stopid


                            if dt_arr:
                                stu.arrival.delay = dt_arr['differenceInMinutes']*60 if 'differenceInMinutes' in dt_arr else 0
                                stu.arrival.time = int(time.mktime(getCompTime(dt_arr).timetuple()))

                            if dt_dep:
                                stu.departure.delay = dt_dep['differenceInMinutes']*60 if 'differenceInMinutes' in dt_dep else 0
                                stu.departure.time = int(time.mktime(getCompTime(dt_dep).timetuple()))

                        #print ix,arr,dep,lkp,dt_arr['differenceInMinutes'],dt_dep['differenceInMinutes']


                    if dt_arr:
                        ent.trip_update.delay = dt_arr['differenceInMinutes']*60 if 'differenceInMinutes' in dt_arr else 0
                    else:
                        ent.trip_update.delay = 0

    def buildGTFSRTMessage(self):
        trains = self.train_dt.getTrainDataCopy()

        msg = gtfs_realtime_pb2.FeedMessage()
        msg.header.gtfs_realtime_version = "1.0"
        msg.header.incrementality = msg.header.FULL_DATASET

        for t in trains:
            if t in self.longdistance:
                self.matchTrainsAndCreateMessage(msg,trains[t],self.longdistance[t])
            else:
                train = trains[t]
                if not 'commuterLineID' in train:
                    continue
                if not train['commuterLineID'] in self.commuter:
                    continue
                self.matchTrainsAndCreateMessage(msg,train,self.commuter[train['commuterLineID']])
            #print msg
            #break
        return msg

if __name__ == '__main__':


    r = requests.get('http://digitransit.fi/route-server/matka.zip', stream=True)
    assert r.status_code == 200
    with open('vr.zip', 'wb') as out_file:
        r.raw.decode_content = True
        shutil.copyfileobj(r.raw, out_file)
    del r
    print 'vr.zip downloaded'


    r = requests.get('http://digitransit.fi/route-server/hsl.zip', stream=True)
    assert r.status_code == 200
    with open('hsl.zip', 'wb') as out_file:
        r.raw.decode_content = True
        shutil.copyfileobj(r.raw, out_file)
    del r
    print 'hsl.zip downloaded'

    trainupdater = None
    trainupdater = railDigitrafficClient(category_filters=set(('Commuter','Long-distance')),keep_timetable_rows=True)
    trainupdater.start()


    hslgtfsprov = railGTFSRTProvider(trainupdater,'hsl.zip')
    ngtfsprov = railGTFSRTProvider(trainupdater,'vr.zip')





    app = Flask(__name__)
    app.debug = DEBUG
    @app.route('/national')
    def national():
        return ngtfsprov.buildGTFSRTMessage().SerializeToString()


    @app.route('/national/debug')
    def nationaldebug():
        return str(ngtfsprov.buildGTFSRTMessage())


    @app.route('/hsl')
    def hsl():
        return hslgtfsprov.buildGTFSRTMessage().SerializeToString()


    @app.route('/hsl/debug')
    def hsldebug():
        return str(hslgtfsprov.buildGTFSRTMessage())

    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0',port=port)