import os, csv, tempfile, re, gzip, pymongo
from ftplib import FTP
from datetime import datetime

#Detailed info on file structure can be found here:
#ftp://ftp.ncdc.noaa.gov/pub/data/gsod/readme.txt
#Missing values: 9999.9
line_struct = {
    'station': '0:6',            #Station number
    'wban': '7:12',              #WBAN number where applicable
    'date': '14:22',             #YYYYMMDD
    'temp': '24:30',             #Mean temperature for the day in degrees Fahrenheit to tenths.
    'temp_count': '31:33',       #Number of observations used to calc. temperature.
    'dew': '35:41',              #Mean dew point for the day
    'dew_count': '42:44',        #Number of observations used to calc. dew point.
    'slp': '46:52',              #Mean sea level pressure for the day in millibars to tenths.
    'slp_count': '53:55',        #Number of observations
    'stp': '57:63',              #Mean station pressure for the day in millibars to tenths.
    'stp_count': '64:66',        #Number of observations
    'visibility': '68:73',       #Mean visibility for the day in miles to tenths. Missing: 999.9
    'visibility_count': '74:76', #Number of observations.
    'wind': '78:83',             #Mean wind speed for the day in knots to tenths. Missing: 999.9
    'wind_count': '84:86',       #Number of observations.
    'max_wind': '88:93',         #Maximum sustained wind speed reported in knots to tenths. Missing 999.9
    'gust': '95:100',            #Maximum wind gust reported in knots to tenths. Missing 999.9
    'max_temp': '102:108',       #Maximum temperature reported. Time of max temp varies by region.
    'max_flag': '108:109',       #Blank indicates max temp was taken from the explicit max temp report...
    'min_temp': '110:116',       #Minimum temperature reported during the day...
    'min_flag': '116:117',       #Blank indicates min temp was taken from the explicit min temp report...
    'precipitation': '118:123',  #Total precipitation (rain and/or melted snow) reported  in inches.. 99.9
    'prcp_flag': '123:124',      #Total precipitation flag.
    'snow': '125:130',           #Snow depth in inches to tenths. Missing: 999.9
    'frshtt': '133:138',         #?
}

def parse_stations(filename, db_host, db_name, db_port=27017, update=False, dry=False):
    """
    Parse station csv file (ftp://ftp.ncdc.noaa.gov/pub/data/gsod/ish-history.csv)
    and add data to mongodb under a collection called stations.
    """
    #Clamp within -180,180 so mongodb accepts the value.
    clamp_loc = lambda x: max(-179.999, min(x, 179.999))
    #Open file
    f = open('data/ish-history.csv', 'r')
    #Connect to database
    connection = pymongo.Connection(db_host, db_port)
    db = connection[db_name]

    #Read info on Stations
    reader = csv.DictReader(f)
    documents = []
    for data in reader:
        #We are only interested in stations with usable lat,lon values.
        if data['LON'] and data['LON'] != '-999999' and data['LAT'] and data['LAT'] != '-999999':
            doc = {
                'usaf': data['USAF'],
                'wban': data['WBAN'],
                'station_name': data['STATION NAME'],
                'country': data['CTRY'],
                'fips': data['FIPS'],
                'state': data['STATE'],
                'call': data['CALL'],
                'loc': { 
                    'long': clamp_loc(float(data['LON'])*0.001),
                    'lat': clamp_loc(float(data['LAT'])*0.001)
                },
                'elevation': float(data['ELEV(.1M)'])*0.1 if data['ELEV(.1M)'] and data['ELEV(.1M)'] != '-99999' else None,
                'begin': datetime.strptime(data['BEGIN'], '%Y%m%d') if data['BEGIN'] else None,
                'end': datetime.strptime(data['END'], '%Y%m%d') if data['END'] else None
            }
            if update:
                db_doc = db.stations.find_one({'usaf': data['USAF'], 'wban': data['wban']})
                if db_doc:
                    db_doc.update(doc)
                    db.stations.save(db_doc)
                else:
                    documents.append(doc)
            else:
                documents.append(doc)
    print "New Stations: %s" % len(documents)
    f.close()
    #Dump to mongodb
    if not dry and documents:
        db.stations.insert(documents, safe=True)


def parse_op(filename, db_host, db_name, db_port=27017, dry=False, update=False):
    fahren_to_celc = lambda f: (f-32)*(5.0/9.0)
    #Connect to database
    connection = pymongo.Connection(db_host, db_port)
    db = connection[db_name]

    #Read file content
    f = gzip.open(filename, 'r') if filename.endswith('.gz') else open(filename, 'r')
    f.readline()
    updated = 0

    data = []
    for line in f.readlines():
        line_dict = {}
        for key, value in line_struct.items():
            #Read line and add values to dict using line_struct.
            i1, i2 = value.split(':')
            line_dict[key] = line.__getslice__(int(i1), int(i2))
            
        #Convert temperature data to Celcius.
        line_dict['date'] = datetime.strptime(line_dict['date'], '%Y%m%d')
        line_dict['temp'] = round(fahren_to_celc(float(line_dict['temp']))) if line_dict['temp'] != '9999.9' else None
        line_dict['max_temp'] = round(fahren_to_celc(float(line_dict['max_temp']))) if line_dict['max_temp'] != '9999.9' else None
        line_dict['min_temp'] = round(fahren_to_celc(float(line_dict['min_temp']))) if line_dict['min_temp'] != '9999.9' else None
        
        if update:
            db_doc = db.observations.find_one({'station': line_dict['station'], 'wban': line_dict['wban'], 'date': line_dict['date']})
            if db_doc and not dry:
                db_doc.update(line_dict)
                db.stations.save(db_doc)
                updated += 1
            else:
                data.append(line_dict)
        else:
            data.append(line_dict)
    f.close()

    print "%s - %s/%s" % (filename, len(data), updated)
    #print "New Observations: %s" % len(data)
    #print "Updated Observations: %s" % updated

    if not dry and data:
        db.observations.insert(data, safe=True)


def update_stations(ftp_host, db_host, db_name):
    conn = FTP(ftp_host)
    conn.login()
    conn.cwd('pub/data/gsod/')

    filename = ''
    if 'ish-history.csv' in conn.nlst():
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            filename = tmp.name
            print tmp.name
            print "Downloading ish-history"
            ret = conn.retrbinary('RETR ish-history.csv', tmp.file.write)
            if ret != '226 Transfer complete':
                print "Unable to fetch ish-history.csv"
            else:
                print "Running parse stations for %s" % filename
                parse_stations(filename, db_host, db_name, update=True)
                print "Cleanup %s" % filename
                print "Fin"
    else:
        print "ish-history.csv not found"
            
    conn.close()
    return False


def update_observations(year, ftp_host, db_host, db_name):
    """
    Update observations from a specified year. Downloads 
    
    """
    reg = re.compile('[0-9]{6}\-[0-9]{5}\-[0-9]{4}\.op\.gz')
    conn = FTP(ftp_host)
    conn.login()
    conn.cwd('pub/data/gsod/%s' % year)
    tmpd = tempfile.mkdtemp()

    for file in conn.nlst():
        if reg.match(file):
            #File is observation file.
            f = open('%s/%s' % (tmpd, file), 'wb')
            ret = conn.retrbinary('RETR %s' % file, f.write)
            f.close()
            if ret != '226 Transfer complete':
                print "Unable to fetch %s" % file
                print ret
            else:
                #print "running parse_op for %s/%s " % (tmpd, file) 
                parse_op("%s/%s" % (tmpd, file), db_host, db_name, update=True)
                #print "Done"
        #break

    print "Done"
