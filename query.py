import pymongo

def get_temperature(lat, lon, date, db_host, db_name, db_port=27017):
    """
    Get temperature values from hottub mongodb.
    """
    connection = pymongo.Connection(db_host, db_port)
    db = connection[db_name]
    
    for stat in db.stations.find({'loc': {'$near': [lat, lon]}}).limit(10):
        obs = db.observations.find_one({'station': stat['usaf'], 'wban': stat['wban'], 'date': date})
        if obs:
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
    return None
