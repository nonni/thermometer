import pymongo, time

connection = pymongo.Connection('127.0.0.1', 27017)
db = connection['hottub']

def get_temperature(lat, lon, date, db_host, db_name, db_port=27017, is_retry=0):
    """
    Get temperature values from hottub mongodb.
    """
    try:
        #connection = pymongo.Connection(db_host, db_port)
        #db = connection[db_name]
    
        for stat in db.stations.find({'loc': {'$near': [lat, lon]}}).limit(10):
            obs = db.observations.find_one({'station': stat['usaf'], 'wban': stat['wban'], 'date': date})
            if obs:
                #connection.disconnect()
                return {
                    'temp': obs['temp'],
                    'max_temp': obs['max_temp'],
                    'min_temp': obs['min_temp'],
                    'lat': stat['loc']['lat'],
                    'lon': stat['loc']['long'],
                    'usaf': stat['usaf'],
                    'wban': stat['wban'],
                    'station_name': stat['station_name']
                }
        #connection.disconnect()
    except pymongo.errors.AutoReconnect:
        if is_retry < 10:
            print "Unable to connect to mongodb, retrying in 5 seconds..."
            time.sleep(5)
            return get_temperature(lat, lon, date, db_host, db_name, db_port=db_port, is_retry=is_retry+1)
        else:
            raise pymongo.errors.AutoReconnect
    return None
