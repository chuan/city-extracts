import json
import os.path
from urllib.parse import urljoin

import requests

from utils import RateLimited


class AngelcoClient():
    """Perform angel.co API requets"""

    base_url = 'https://api.angel.co/1/'

    def __init__(self, accessToken, dataDir):
        self.accessToken = accessToken
        self.dataDir = dataDir

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

    def dump_all_investors(self, tag_id):
        path = 'tags/{0:d}/users'.format(tag_id)
        params = {'investors': 'by_residence'}
        total = -1
        p = 1
        while (True):
            data = self.get(path, params)
            if (total < 0):
                total = self.get(path)['total']
            filename = os.path.join(
                self.dataDir,
                'investors/{0:d}/{1:d}.json'.format(tag_id, p))
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))

            with open(filename, 'w') as outfile:
                json.dump(data, outfile)

            lp = data['last_page']

            print('Dumping {0:d} of a total {1:d} pages to {2}.'.format(
                p,
                lp,
                filename))
            if (lp <= p):
                break
            p += 1
        return total

    def get_investor_count(self, tag_id):
        # API doc: https://angel.co/api/spec/users
        path = 'tags/{0:d}/users'.format(tag_id)
        params = {'investors': 'by_residence'}
        return self.get(path, params)['total']
