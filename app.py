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
import math
import shutil
import logging

import zipfile
import unicodecsv

DEBUG = False


NO_ALERTS = 0
AGGREGATED_ALERTS = 1
FULL_ALERTS = 2

if DEBUG:
    import cPickle

def getCompTime(eventrow):
    if 'actualTime' in eventrow:
        return eventrow['actualTime']
    if 'liveEstimateTime' in eventrow:
        return eventrow['liveEstimateTime']
    if 'scheduledTime' in eventrow:
        return eventrow['scheduledTime']

def downloadGTFS(url, zip_in_zips):
    while 1:
        r = requests.get(url, stream=True)

        if r.status_code == 200:
            with open('tmp.zip', 'wb') as out_file:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, out_file)
            del r
            zf = zipfile.ZipFile('tmp.zip')
            for name in zip_in_zips:
                print 'Extracting ' + name
                zf.extract(name)
            del zf
            break
        else:
            logging.warning('Could not load router-zip from ' + url + ' http-status:' + str(r.status_code))
            time.sleep(5)


def getCategoryCodes(detailed=False):
    url = 'http://rata.digitraffic.fi/api/v1/metadata/cause-category-codes'
    if detailed:
        url = 'http://rata.digitraffic.fi/api/v1/metadata/detailed-cause-category-codes'

    r = requests.get(url)

    codes = r.json()

    rcodes = {}
    for c in codes:
        if detailed:
            rcodes[c['detailedCategoryCode']] = c['detailedCategoryName']
        else:
            rcodes[c['categoryCode']] = c['categoryName']

    return rcodes


def translateAlertCause(cause):
        '''
        UNKNOWN_CAUSE
        OTHER_CAUSE
        TECHNICAL_PROBLEM
        STRIKE
        DEMONSTRATION
        ACCIDENT
        HOLIDAY
        WEATHER
        MAINTENANCE
        CONSTRUCTION
        POLICE_ACTIVITY
        MEDICAL_EMERGENCY
        '''

        known_detailed_reasons = {
            'E1':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'E2':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'E3':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'E4':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'E5':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'E6':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'E7':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'H1':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'H2':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'H3':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'I1':gtfs_realtime_pb2.Alert.WEATHER,'I2':gtfs_realtime_pb2.Alert.WEATHER,
            'I3':gtfs_realtime_pb2.Alert.POLICE_ACTIVITY,'I4':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'J1':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'J2':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'J3':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'J4':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'J5':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'K1':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'K2':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'K3':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'K4':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'K5':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'K6':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'K7':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'L1':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'L2':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'L3':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'L4':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'L5':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'L6':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'L7':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'L8':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'M1':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'M2':gtfs_realtime_pb2.Alert.POLICE_ACTIVITY,
            'M3':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'M4':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'M5':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'M6':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'O1':gtfs_realtime_pb2.Alert.ACCIDENT,'O2':gtfs_realtime_pb2.Alert.ACCIDENT,
            'O3':gtfs_realtime_pb2.Alert.ACCIDENT,'O4':gtfs_realtime_pb2.Alert.ACCIDENT,
            'P1':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'P2':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'P3':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'P4':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'P5':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'P6':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'P7':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'R1':gtfs_realtime_pb2.Alert.MAINTENANCE,'R2':gtfs_realtime_pb2.Alert.MAINTENANCE,
            'R3':gtfs_realtime_pb2.Alert.MAINTENANCE,'R4':gtfs_realtime_pb2.Alert.MAINTENANCE,
            'S1':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'S2':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'S3':gtfs_realtime_pb2.Alert.CONSTRUCTION,'S4':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'T1':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'T2':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'T3':gtfs_realtime_pb2.Alert.OTHER_CAUSE,'T4':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'V1':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'V2':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'V3':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,'V4':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
        }


        known_reasons = {
            'E':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'H':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'I':gtfs_realtime_pb2.Alert.WEATHER,
            'J':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'K':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'L':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'M':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'O':gtfs_realtime_pb2.Alert.ACCIDENT,
            'P':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'R':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'S':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
            'T':gtfs_realtime_pb2.Alert.OTHER_CAUSE,
            'V':gtfs_realtime_pb2.Alert.TECHNICAL_PROBLEM,
        }
        if u'detailedCategoryCode' in cause:
            return known_detailed_reasons[cause[u'detailedCategoryCode']] if cause[u'detailedCategoryCode'] in known_detailed_reasons else gtfs_realtime_pb2.Alert.UNKNOWN_CAUSE
        elif u'categoryCode' in cause:
            return known_reasons[cause[u'categoryCode']] if cause[u'categoryCode'] in known_reasons else gtfs_realtime_pb2.Alert.UNKNOWN_CAUSE
        else:
            return None


def getTrainSchedules(date=None):
    if not date:
        date = datetime.date.today()

    url = 'http://rata.digitraffic.fi/api/v1/schedules?departure_date=%s' % date.strftime('%Y-%m-%d')

    r = requests.get(url)
    scheduledata = r.json()
    r.close()

    return scheduledata

class railDigitrafficClient(threading.Thread):
    def __init__(self,category_filters=None,type_filters=None,keep_timetable_rows=False):
        super(railDigitrafficClient,self).__init__()
        self.daemon = True
        self.running = True
        self.trains = {}
        self.lines = {}
        self.latest_version = None
        self.data_loaded = False

        self.cause_category_codes = getCategoryCodes()
        self.cause_detailed_category_codes = getCategoryCodes(detailed=True)

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

    def translateCauses(self,eventrow):
        if 'causes' in eventrow and len(eventrow['causes']) > 0:
            for c in eventrow['causes']:
                c[u'categoryText'] = self.cause_category_codes[c[u'categoryCode']]
                c[u'detailedCategoryText'] = self.cause_detailed_category_codes[c[u'detailedCategoryCode']] if 'detailedCategoryCode' in c else None

        return eventrow
    def getTrainDataCopy(self):
        while not self.data_loaded:
            time.sleep(2)
        return copy.copy(self.trains)

    def handleTimetableRows(self,t):

        t['timeTableRows'] = map(self.convertTimetable,t['timeTableRows'])
        t['timeTableRows'] = map(self.translateCauses,t['timeTableRows'])

        t['first'] = t['timeTableRows'][0]
        t['last'] = t['timeTableRows'][-1]

        if not self.keep_timetable_rows:
            del t['timeTableRows']

        return t

    def getCancelledSchedules(self,date=None):
        scheduledata = getTrainSchedules(date)

        cancelled_schedules = []
        for train in scheduledata:
            if self.category_filters and not train['trainCategory'] in self.category_filters:
                continue

            if train['runningCurrently']:
                continue

            if not train['cancelled']:
                continue

            train = self.handleTimetableRows(train)

            cancelled_schedules.append(train)

        return cancelled_schedules


    def run(self):
        last_schedule_update = None

        while self.running:
            self.data_loaded = False
            params = None
            if self.latest_version != None:
                params = {'version':self.latest_version}

            now = datetime.datetime.now()
            cancelled = []

            if not last_schedule_update or now-last_schedule_update > datetime.timedelta(minutes=5):
                try:
                    cancelled = self.getCancelledSchedules()
                    last_schedule_update = now
                    print 'schedules updated',now
                except:
                    print 'schedule update failed'

            for t in cancelled:
                self.trains[t['trainNumber']] = t

            try:
                r = requests.get('http://rata.digitraffic.fi/api/v1/live-trains',params=params)
                traindata = r.json()
                r.close()
            except:
                print 'live-trains request failed'
                time.sleep(5)
                continue

            del r
            tn = tn2 = 0
            for t in traindata:
                tn+=1
                if self.category_filters and not t['trainCategory'] in self.category_filters:
                    continue

                if self.type_filters and not t['trainType'] in self.type_filters:
                    continue

                # Juna saapunut maaranpaahan
#                if 'actualTime' in t['timeTableRows'][-1]:
#                    continue

                # Juna ei ole viel lahtenut -> ei ennusteita
                if not t['runningCurrently'] and not t['cancelled']:
                    continue

#                if not 'actualTime' in t['timeTableRows'][0]:
#                    continue

                t = self.handleTimetableRows(t)

                if not self.latest_version or t['version'] > self.latest_version:
                    self.latest_version = t['version']

                self.trains[t['trainNumber']] = t
                tn2 += 1

            del traindata

            tns = self.trains.keys()
            now = datetime.datetime.utcnow().replace(tzinfo=self.utc)
            past_limit = now-datetime.timedelta(hours=4)
            future_limit = now+datetime.timedelta(hours=4)
            for tn in tns:
                if getCompTime(self.trains[tn]['first']) > future_limit:
                    del self.trains[tn]
                    continue
                if getCompTime(self.trains[tn]['last']) < past_limit:
                    del self.trains[tn]
                    continue

            self.data_loaded = True
            time.sleep(10)
        print 'Done!'


class stop2stationResolver(object):
    dt_stations = {}
    name2station = {}
    stop2station = {}

    match_failed = set()

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
        if gtfsid in self.match_failed:
            return None

        if gtfsid in self.stop2station:
            return self.stop2station[gtfsid]

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
            self.match_failed.add(stop['stop_id'])
            return None

        self.stop2station[gtfsid] = nearest[1][0]
        return nearest[1][0]

def loadGTFSRailTripData(gtfs_package = 'vr.zip'):

    if DEBUG:
        tmp_filename = gtfs_package.replace('.zip','_tmp.dat')
        if os.path.exists(tmp_filename):
            with open(tmp_filename,'rb') as f:
                ret = cPickle.load(f)

            print 'GTFS DATA LOADED FROM DISK'
            return ret

    st = time.time()
    wanted_stopids = set([])
    wanted_routeids = set([])
    wanted_tripids = set([])
    wanted_services = set([])

    routes = {}
    trips = {}
    stops = {}
    services = {}

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
            wanted_services.add(l['service_id'])
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


    with zf.open('calendar.txt','r') as f:
        csvf = unicodecsv.reader(f, delimiter=',', quotechar='"')
        headers = None
        for l in csvf:
            if headers == None:
                headers = l
                continue
            l = dict(zip(headers,l))

            if not l['service_id'] in wanted_services:
                continue

            wdaystr = ''
            for wdk in ('monday','tuesday','wednesday','thursday','friday','saturday','sunday'):
                wdaystr += l[wdk]
                del l[wdk]
            l['weekdays'] = wdaystr

            services[l['service_id']] = l
            services[l['service_id']]['dates'] = {}


    print gtfs_package,len(services),'calendars loaded'

    service_date_count = 0
    with zf.open('calendar_dates.txt','r') as f:
        csvf = unicodecsv.reader(f, delimiter=',', quotechar='"')
        headers = None
        for l in csvf:
            if headers == None:
                headers = l
                continue
            l = dict(zip(headers,l))

            if not l['service_id'] in wanted_services:
                continue

            if not l['service_id'] in services:
                services[l['service_id']] = {'dates':{}}

            services[l['service_id']]['dates'][l['date']] = l['exception_type'] == '1'
            service_date_count += 1
    print gtfs_package,service_date_count,'calendar dates loaded'
    zf.close()


    print gtfs_package,'reading done. Took:',time.time()-st

    if DEBUG:
        with open(tmp_filename,'wb') as f:
            cPickle.dump((routes,trips,stops,services),f,-1)
            print 'GTFS DATA WRITTEN TO DISK'


    return routes,trips,stops,services

def gtfstime2timedelta(gtfstime):
    t = map(int,gtfstime.split(':'))

    return datetime.timedelta(seconds=t[0]*3600+t[1]*60+t[2])


def servicesToDatedict(services):
    dates = {}

    for sk in services:

        service = services[sk]

        if all((k in service for k in ('start_date','end_date','weekdays'))):
            start = datetime.datetime.strptime(service['start_date'],'%Y%m%d')
            end = datetime.datetime.strptime(service['end_date'],'%Y%m%d')
            cdate = start
            while cdate <= end:

                if service['weekdays'][cdate.weekday()] == '1':
                    cdate_date = cdate.date()
                    if not cdate_date in dates:
                        dates[cdate_date] = set([])
                    dates[cdate_date].add(sk)

                cdate += datetime.timedelta(days=1)

        if 'dates' in service and len(service['dates']) > 0:
            for d in service['dates']:

                date = datetime.datetime.strptime(d,'%Y%m%d')

                if service['dates'][d]:
                    if not date in dates:
                        dates[date] = set()
                    dates[date].add(sk)
                else:
                    if not date in dates:
                        continue
                    print sk,'removed from',date
                    dates[date].discard(sk)
    return dates

class railGTFSRTProvider(object):
    def __init__(self,train_dt=None,gtfs_source='vr.zip'):
        self.s2sr = stop2stationResolver()
        self.train_dt = train_dt
        self.stop2station = {}
        self.entid = 1

        self.routes, self.trips, self.stops, services = loadGTFSRailTripData(gtfs_source)


        self.dateservices = servicesToDatedict(services)

        self.handleGTFSRouteData()

    def handleGTFSRouteData(self):
        self.longdistance = {}
        self.commuter = {}

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

    def getGTFSTripsForTrain(self,train):
        if train['trainNumber'] in self.longdistance:
            return self.longdistance[train['trainNumber']]
        else:
            if 'commuterLineID' in train and train['commuterLineID'] in self.commuter:
                return self.commuter[train['commuterLineID']]

        if False and DEBUG:
            print 'GTFS routes/trips not found for train',train['trainNumber'],'(',train['commuterLineID'],')'
        return None


    def matchTrainsAndCreateMessage(self,msg,train,alerts=False,fuzzy=False,debug=False):
        gtfs_trips = self.getGTFSTripsForTrain(train)

        if not gtfs_trips:
            return False


        today = datetime.datetime.now().replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=tzlocal())
        station_times = {}

        #ix 1
        for tr in train['timeTableRows']:
            if not tr['trainStopping']:
                continue

            if not tr['commercialStop']:
                continue

            station_times[(tr['stationShortCode'],tr['type'],tr['scheduledTime'].time().replace(second=0))] = tr



        tk = (train['first']['stationShortCode'],train['first']['scheduledTime'].time().replace(second=0))
        date_services = self.dateservices[datetime.datetime.strptime(train['departureDate'],'%Y-%m-%d').date()]

        seen_combinations = set()

        for r in gtfs_trips:
            routeid = r[0]
            for t in r[1]:
                if t != tk:
                    continue

                if fuzzy:
                    if (routeid,tk) in seen_combinations:
                        continue
                    seen_combinations.add((routeid,tk))

                for tripid,stops in r[1][t]:
                    ix = 1
                    serviceid = self.trips[tripid]['service_id']
                    if not serviceid in date_services:
                        continue

                    stus = []
                    dbg = []
                    skipped = False
                    for arr,dep,station,stopid in stops:
                        arr =(today+gtfstime2timedelta(arr)).time()
                        dep =(today+gtfstime2timedelta(dep)).time()

                        dt_arr = dt_dep = None
                        lkp = station
                        dbg.append(str(((lkp,'ARRIVAL',arr),(lkp,'ARRIVAL',arr) in station_times)))
                        if (lkp,'ARRIVAL',arr) in station_times:
                            dt_arr = station_times[(lkp,'ARRIVAL',arr)]
                        elif (lkp,'DEPARTURE',arr) in station_times:
                            dt_arr = station_times[(lkp,'DEPARTURE',arr)]

                        dbg.append(str(((lkp,'DEPARTURE',dep),(lkp,'DEPARTURE',dep) in station_times)))

                        if (lkp,'DEPARTURE',dep) in station_times:
                            dt_dep = station_times[(lkp,'DEPARTURE',dep)]

                        if ix == 1 and dt_arr == None:
                            dt_arr = dt_dep

                        if dt_dep == None and ix == len(stops):
                            dt_dep = dt_arr


                        if dt_arr and dt_dep:
                            stus.append((ix,stopid,dt_arr,dt_dep))
                        else:
                            dbg.append(str(('skipped',dt_arr,dt_dep)))
                            dbg.append(str((arr,dep,station,stopid)))
                            #skipped = True
                            #break

                        ix+=1

                    if skipped:
                        if DEBUG:
                            print routeid,tripid,train['trainNumber'],train['commuterLineID'] if 'commuterLineID' in train else '','invalid match'
                            pprint.pprint(station_times)
                            pprint.pprint(stops)
                            pprint.pprint(dbg)
                        continue

                    ent = msg.entity.add()
                    ent.id = str(self.entid)
                    self.entid+=1

                    feed_tripid = tripid
                    if DEBUG or debug:
                        feed_tripid += '-' + str(train['trainNumber']) + ('-%s' % train['commuterLineID'] if 'commuterLineID' in train and train['commuterLineID'] != '' else '')

                    if not fuzzy:
                        ent.trip_update.trip.trip_id = feed_tripid
                    ent.trip_update.trip.route_id = routeid
                    ent.trip_update.trip.start_time = train['first']['scheduledTime'].replace(second=0).strftime('%H:%M:%S')
                    ent.trip_update.trip.start_date = train['departureDate'].replace('-','')
                    ent.trip_update.trip.direction_id = int(self.trips[tripid]['direction_id']) if 'direction_id' in self.trips[tripid] else 0
                    ent.trip_update.trip.schedule_relationship = ent.trip_update.trip.CANCELED if train['cancelled'] else ent.trip_update.trip.SCHEDULED
                    ent.trip_update.timestamp = int(time.time())

                    alert_ent = None
                    trip_cause = None
                    trip_messages = []

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

                            if (not dt_arr or dt_arr['cancelled']) and (not dt_dep or dt_dep['cancelled']):
                                stu.schedule_relationship = stu.SKIPPED

                            if alerts > NO_ALERTS:
                                stop_messages = []
                                if dt_arr and 'causes' in dt_arr and len(dt_arr['causes']) > 0:
                                    for c in dt_arr['causes']:
                                        stop_messages.append(u'%s: %s%s' % (
                                            self.s2sr.dt_stations[dt_arr['stationShortCode']][1],
                                            c[u'categoryText'],
                                            (' / %s' % c[u'detailedCategoryText']) if c[u'detailedCategoryText'] else ''
                                            ))
                                        trip_cause = translateAlertCause(c)

                                if dt_dep and 'causes' in dt_dep and len(dt_dep['causes']) > 0:
                                    for c in dt_dep['causes']:
                                        stop_messages.append(u'%s: %s%s' % (
                                            self.s2sr.dt_stations[dt_dep['stationShortCode']][1],
                                            c[u'categoryText'],
                                            (' / %s' % c[u'detailedCategoryText']) if c[u'detailedCategoryText'] else ''
                                            ))
                                        trip_cause = translateAlertCause(c)

                                stop_messages = list(set(stop_messages))
                                if len(stop_messages) > 0:
                                    if alerts == FULL_ALERTS:
                                        alert_ent = msg.entity.add()
                                        alert_ent.id = str(self.entid)
                                        self.entid+=1
                                        ie = alert_ent.alert.informed_entity.add()
                                        alert_trip = ie.trip.CopyFrom(ent.trip_update.trip)
                                        ie.stop_id = stopid
                                        alert_ent.alert.effect = alert_ent.alert.SIGNIFICANT_DELAYS
                                        if dt_dep and len(dt_dep['causes']) > 0:
                                            alert_ent.alert.cause = translateAlertCause(dt_dep['causes'][-1])
                                        elif dt_arr and len(dt_arr['causes']) > 0:
                                            alert_ent.alert.cause = translateAlertCause(dt_arr['causes'][-1])
                                        else:
                                            raise ValueError('This should\'t newer happen?')

                                        if train['commuterLineID'] == '':
                                            info_urls = (
                                                ('https://www.vr.fi/cs/vr/fi/liikennetilanne','fi'),
                                                ('https://www.vr.fi/cs/vr/sv/trafikinfo','sv'),
                                                ('https://www.vr.fi/cs/vr/en/traffic_info','en'),
                                            )
                                        else:
                                            info_urls = (
                                                ('https://www.hsl.fi/','fi'),
                                                ('https://www.hsl.fi/sv','sv'),
                                                ('https://www.hsl.fi/en','en'),
                                            )

                                        for url,lang in info_urls:
                                            aurl = alert_ent.alert.url.translation.add()
                                            aurl.text = url
                                            aurl.language = lang

                                        dtext = alert_ent.alert.description_text.translation.add()
                                        dtext.text = '\n'.join(stop_messages)
                                    elif alerts == AGGREGATED_ALERTS:
                                        trip_messages.append('\n'.join(stop_messages))

                        #print ix,arr,dep,lkp,dt_arr['differenceInMinutes'],dt_dep['differenceInMinutes']


                    if dt_arr:
                        ent.trip_update.delay = dt_arr['differenceInMinutes']*60 if 'differenceInMinutes' in dt_arr else 0
                    else:
                        ent.trip_update.delay = 0

                    if alerts == AGGREGATED_ALERTS and len(trip_messages) > 0 and trip_cause != None:
                        alert_ent = msg.entity.add()
                        alert_ent.id = str(self.entid)
                        self.entid+=1
                        ie = alert_ent.alert.informed_entity.add()
                        alert_trip = ie.trip.CopyFrom(ent.trip_update.trip)
                        alert_ent.alert.effect = alert_ent.alert.SIGNIFICANT_DELAYS
                        alert_ent.alert.cause = trip_cause

                        if train['commuterLineID'] == '':
                            info_urls = (
                                ('https://www.vr.fi/cs/vr/fi/liikennetilanne','fi'),
                                ('https://www.vr.fi/cs/vr/sv/trafikinfo','sv'),
                                ('https://www.vr.fi/cs/vr/en/traffic_info','en'),
                            )
                        else:
                            info_urls = (
                                ('https://www.hsl.fi/','fi'),
                                ('https://www.hsl.fi/sv','sv'),
                                ('https://www.hsl.fi/en','en'),
                            )
                        for url,lang in info_urls:
                            aurl = alert_ent.alert.url.translation.add()
                            aurl.text = url
                            aurl.language = lang

                        dtext = alert_ent.alert.description_text.translation.add()
                        dtext.text = '\n\n'.join(trip_messages)



    def buildGTFSRTMessage(self,alerts=0,fuzzy=False,debug=False):
        trains = self.train_dt.getTrainDataCopy()

        msg = gtfs_realtime_pb2.FeedMessage()
        msg.header.gtfs_realtime_version = "1.0"
        msg.header.incrementality = msg.header.FULL_DATASET

        #return pprint.pformat(trains)

        for t in trains:
            self.matchTrainsAndCreateMessage(msg,trains[t],alerts=alerts,fuzzy=fuzzy,debug=debug)

        return msg



if __name__ == '__main__':
    VR_ZIP = 'router-finland/matka.zip'
    HSL_ZIP = 'router-finland/hsl.zip'
    router_zip_url = os.getenv('ROUTER_ZIP_URL', 'http://api.digitransit.fi/routing-data/v1/router-finland.zip')

    downloadGTFS(router_zip_url, [VR_ZIP, HSL_ZIP])

    trainupdater = None
    trainupdater = railDigitrafficClient(category_filters=set(('Commuter','Long-distance')),keep_timetable_rows=True)

    trainupdater.start()

    hslgtfsprov = railGTFSRTProvider(trainupdater, HSL_ZIP)
    ngtfsprov = railGTFSRTProvider(trainupdater, VR_ZIP)

    app = Flask(__name__)
    app.debug = DEBUG
    @app.route('/national',defaults={'debug':0,'fuzzy':0,'alerts':0})
    @app.route('/national/<int:alerts>/<int:fuzzy>/<int:debug>')
    def national(alerts,fuzzy,debug):
        fuzzy = fuzzy == 1
        debug = debug == 1

        if not debug:
            return ngtfsprov.buildGTFSRTMessage(alerts=alerts,fuzzy=fuzzy,debug=debug).SerializeToString()
        else:
            return str(ngtfsprov.buildGTFSRTMessage(alerts=alerts,fuzzy=fuzzy,debug=debug))

    @app.route('/hsl',defaults={'debug':0,'fuzzy':0,'alerts':0})
    @app.route('/hsl/<int:alerts>/<int:fuzzy>/<int:debug>')
    def hsl(alerts,fuzzy,debug):
        fuzzy = fuzzy == 1
        debug = debug == 1

        if not debug:
            return hslgtfsprov.buildGTFSRTMessage(alerts=alerts,fuzzy=fuzzy,debug=debug).SerializeToString()
        else:
            return str(hslgtfsprov.buildGTFSRTMessage(alerts=alerts,fuzzy=fuzzy,debug=debug))

    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0',port=port)