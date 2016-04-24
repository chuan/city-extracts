import configparser
import getpass
import io
import json
import os
import os.path
import subprocess
import sys
import time
from urllib.parse import urljoin

import psycopg2
import requests
from factual import Factual

config = None


def RateLimited(maxPerSecond):
    """Poor man's rate limiting decorator"""
    minInterval = 1.0 / float(maxPerSecond)

    def decorate(func):
        lastTimeCalled = [0.0]

        def rateLimitedFunction(*args, **kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait > 0:
                time.sleep(leftToWait)
            ret = func(*args, **kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate


def import_osm_data(city):
    """Import osm data into PostGIS.
    Assume PostgreSQL and PostGIS running on localhost."""
    global config
    dataDir = config['OpenStreetMap']['DataDir']

    db = city['osm']['db']
    url = city['osm']['link']
    filename = os.path.join(dataDir, url.split('/')[-1])
    if (not os.path.isfile(filename)):
        subprocess.run(['wget', '-P', dataDir, url], check=True)

        subprocess.run(['sudo', '-u', 'postgres', 'createdb', '-O',
                        getpass.getuser(), db, '-E', 'UTF-8'], check=True)

        subprocess.run(['sudo', '-u', 'postgres', 'psql', '-d', db, '-c',
                        'CREATE EXTENSION postgis; CREATE EXTENSION hstore;'],
                       check=True)

        subprocess.run(['osm2pgsql', '--create', '--slim', '--database',
                        db, filename],
                       check=True, cwd=dataDir)


def check_dirs(dirs):
    """Check if the given dirs exist. Create the dir if it does not exist.
    Exit the program if anything else, e.g. file or link, is on the path."""
    for d in dirs:
        if (os.path.isdir(d)):
            pass
        elif (not os.path.exists(d)):
            os.makedirs(d)
        else:
            print(
                'Invalid data directory \'{0}\'.'.format(d),
                file=sys.stderr)
            system.exit(1)


class FactualClient():
    """Perform factual api requests"""
    # http://developer.factual.com/working-with-categories/

    factual = None

    def __init__(self):
        global config
        self.factual = Factual(config['factual']['key'],
                               config['factual']['secret'])

    def get_city_filter(self, d):
        filter = {'$and': []}
        for k, v in d.items():
            filter['$and'].append({k: {'$eq': v}})
        return filter

    def get_total_count(self, filter):
        return (self.factual
                .table('places')
                .filters(filter)
                .include_count(True)
                .total_row_count())

    def get_data(self, filter):
        return (self.factual
                .table('places')
                .filters(filter)
                .data())

    def get_category_count(self, cityquery, categoryIds):
        filter = self.get_city_filter(cityquery)
        filter['$and'].append({'category_ids': {'$includes_any': categoryIds}})
        return self.get_total_count(filter)

    # 29    Community and Government > Education > Colleges and Universities
    def get_college_count(self, cityquery):
        return self.get_category_count(cityquery, [29])

    # 181   Businesses and Services > Metals
    # 183   Businesses and Services > Petroleum
    # 184   Businesses and Services > Plastics
    # 186   Businesses and Services > Rubber
    # 190   Businesses and Services > Textiles
    # 192   Businesses and Services > Welding
    # 207   Businesses and Services > Automation and Control Systems
    # 208   Businesses and Services > Chemicals and Gasses
    # 213   Businesses and Servicess > Engineering
    # 268   Businesses and Services > Leather
    # 275   Businesses and Services > Manufacturing
    # 301   Businesses and Services > Renewable Energy
    # 447   Businesses and Services > Construction
    # 460   Businesses and Services > Technology
    def get_industry_count(self, cityquery):
        ids = [181, 183, 184, 186, 190, 192, 207, 208, 213, 268, 275, 301, 447,
               460]
        return self.get_category_count(cityquery, ids)

    # 218   Businesses and Services > Financial > Banking and Finance > ATMs
    def get_atm_count(self, cityquery):
        return self.get_category_count(cityquery, [218])

    # 221   Businesses and Services > Financial > Banking and Finance
    def get_bank_count(self, cityquery):
        return self.get_category_count(cityquery, [221])


class OsmClient():
    """Perform OSM queries"""

    connection = None

    def __init__(self, db, user):
        self.connection = psycopg2.connect(
            'dbname={:s} user={:s}'.format(db, user))

    def __del__(self):
        self.connection.close()

    def get_atm_count(self):
        return self.get_amenity_count('atm')

    def get_bank_count(self):
        return self.get_amenity_count('bank')

    def get_library_count(self):
        return self.get_amenity_count('library')

    def get_college_count(self):
        return self.get_amenity_count('college')

    def get_university_count(self):
        return self.get_amenity_count('university')

    def get_pub_count(self):
        return self.get_amenity_count('pub')

    def get_bar_count(self):
        return self.get_amenity_count('bar')

    def get_restaurant_count(self):
        return self.get_amenity_count('restaurant')

    def get_cafe_count(self):
        return self.get_amenity_count('cafe')

    def get_station_count(self):
        return self.get_public_transport_count('station')

    def get_amenity_count(self, amenity):
        # All OSM amentiy types: http://wiki.openstreetmap.org/wiki/Key:amenity
        cur = self.connection.cursor()
        cur.execute(
            """SELECT COUNT(*) FROM planet_osm_point """ +
            """WHERE amenity=%s;""",
            (amenity,))
        return cur.fetchone()[0]

    def get_public_transport_count(self, key):
        # OSM public transport doc:
        # http://wiki.openstreetmap.org/wiki/Key:public_transport
        cur = self.connection.cursor()
        cur.execute(
            """SELECT COUNT(*) FROM planet_osm_point """ +
            """WHERE public_transport=%s;""",
            (key,))
        return cur.fetchone()[0]


class AngelcoClient():
    """Perform angel.co API requets"""

    base_url = 'https://api.angel.co/1/'

    def __init__(self):
        global config
        self.accessToken = config['angel.co']['AccessToken']
        self.dataDir = config['angel.co']['DataDir']

    @RateLimited(0.5)
    def get(self, path, params={}):
        """Helper function to get data from angel.co API.
        Requests are limited to 2000 requests per hour (~0.5/sec) per token."""
        if (not params):
            params = {'access_token': self.accessToken}
        else:
            params['access_token'] = self.accessToken
        url = urljoin(self.base_url, path)
        r = requests.get(url, params=params)
        return r.json()

    def get_startup_count(self, tag_id):
        # API doc: https://angel.co/api/spec/startups
        path = 'tags/{0:d}/startups'.format(tag_id)
        return self.get(path)['total']

    def dump_all_startups(self, tag_id):
        path = 'tags/{0:d}/startups'.format(tag_id)
        total = -1
        p = 1
        while (True):
            data = self.get(path, {'page': p})
            if (total < 0):
                total = self.get(path)['total']
            filename = os.path.join(
                self.dataDir,
                'cities/{0:d}/startup{1:d}.json'.format(tag_id, p))
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))

            with open(filename, 'w') as outfile:
                json.dump(data, outfile)

            for s in data['startups']:
                print('startup_id: {0:d}'.format(s['id']))
                self.dump_founders(s['id'])

            lp = data['last_page']
            print('Dumping {0:d} of a total {1:d} pages to {2}.'.format(
                p,
                lp,
                filename))
            if (lp <= p):
                break
            p += 1
        return total

    def dump_founders(self, startup_id):
        # API doc: https://angel.co/api/spec/startup_roles
        path = 'startup_roles'
        params = {'v': 1,
                  'role': 'founder',
                  'startup_id': startup_id}
        data = self.get(path, params)
        filename = os.path.join(self.dataDir, 'startups',
                                '{0:d}_founders.json'.format(startup_id))
        with open(filename, 'w') as outfile:
            json.dump(data, outfile)

    def get_investor_count(self, tag_id):
        # API doc: https://angel.co/api/spec/users
        path = 'tags/{0:d}/users'.format(tag_id)
        params = {'investors': 'by_residence'}
        return self.get(path, params)['total']


def main():
    """Main method"""
    global config
    config = configparser.ConfigParser()
    config.read('extract.ini')

    check_dirs([config['OpenStreetMap']['DataDir'],
                config['angel.co']['DataDir'],
                os.path.join(config['angel.co']['DataDir'], 'cities'),
                os.path.join(config['angel.co']['DataDir'], 'startups')])

    cities = None
    cities_file = config['main']['input']
    output_file = config['main']['output']

    with open(output_file, 'wt') as out:

        with io.open(cities_file, 'r', encoding='utf-8') as f:
            cities = json.load(f)['cities']

        for city in cities:
            import_osm_data(city)

        osm_amenity = ['atm', 'bank', 'library', 'college', 'university',
                       'pub', 'bar', 'restaurant', 'cafe']
        osm_public_transport = ['station', 'platform']
        angel_data = ['startups', 'investors']
        factual_data = ['finance', 'university', 'industry']

        print('cities', end='', file=out)
        for col in osm_amenity + osm_public_transport:
            print(', osm_{0}'.format(col), end='', file=out)
        for col in angel_data:
            print(', angelco_{0}'.format(col), end='', file=out)
        for col in factual_data:
            print(', factual_{0}'.format(col), end='', file=out)
        print(file=out)

        angel = AngelcoClient()
        factual = FactualClient()
        for city in cities:
            db = city['osm']['db']
            osm = OsmClient(db, getpass.getuser())
            city_tag = city['angelco']['tag_id']
            # Print city name
            print(city['name'], end='', file=out)

            # Print OSM count
            for amenity in osm_amenity:
                print(', {:d}'.format(osm.get_amenity_count(amenity)),
                      end='', file=out)
            for key in osm_public_transport:
                print(', {:d}'.format(osm.get_public_transport_count(key)),
                      end='', file=out)

            # Print angelco count
            print(', {:d}'.format(angel.get_startup_count(city_tag)),
                  end='', file=out)
            print(', {:d}'.format(angel.get_investor_count(city_tag)),
                  end='', file=out)
            if (config['angel.co'].getboolean('DumpData')):
                angel.dump_all_startups(city_tag)

            # Print factual count
            if (city['factual']):
                print(', {:d}'.format(
                    factual.get_bank_count(city['factual'])),
                    end='', file=out)

                print(', {:d}'.format(
                    factual.get_college_count(city['factual'])),
                    end='', file=out)

                print(', {:d}'.format(
                    factual.get_industry_count(city['factual'])),
                    end='', file=out)
            else:
                for col in factual_data:
                    print(', NA', end='', file=out)

            print(file=out)
            print('{0} finished.'.format(city['name']))
        print('Done.')

if __name__ == '__main__':
    main()
