import getpass
import os.path
import subprocess

import psycopg2


def import_osm_data(city, dataDir):
    """Import osm data into PostGIS.
    Assume PostgreSQL and PostGIS running on localhost."""

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
