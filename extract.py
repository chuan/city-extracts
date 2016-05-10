import configparser
import getpass
import io
import json
import os
import os.path

from angelco import AngelcoClient
from factual_client import FactualClient
from osm import OsmClient, import_osm_data
from utils import check_dirs

config = None


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
            import_osm_data(city, config['OpenStreetMap']['DataDir'])

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

        angel = AngelcoClient(config['angel.co']['AccessToken'],
                              config['angel.co']['DataDir'])

        factual = FactualClient(config['factual']['key'],
                                config['factual']['secret'])

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
