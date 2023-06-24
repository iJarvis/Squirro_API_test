import argparse
import logging
import requests
import json
from configparser import ConfigParser
from collections import deque
from collections.abc import MutableMapping
import time

"""
Skeleton for Squirro Delivery Hiring Coding Challenge
August 2021
"""


log = logging.getLogger(__name__)


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self):
        pass

    def connect(self, inc_column=None, max_inc_value=None):
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    # Pop n(batch_size) items from the front of the queue. Note that this is only called when queue has at least 'batch_size'
    # number of items, hence no checks for size/emptiness are made
    def getn(self, q, n):
        result = [q.pop()]
        while len(result) < n:
            result.append(q.pop())
        return result
    
    # Simple function to flatten a dictionary, nested key names are appended, separated by a '.'
    def flatten(self, d, parent_key =''):
        items = []
        for k, v in d.items():
            new_key = parent_key + '.' + k if parent_key else k
    
            if isinstance(v, MutableMapping):
                items.extend(self.flatten(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)

    # Generator: Sends Get request to API endpoint and yields 'batch_size' data at once when enumerating over it.
    # NYT API returns 10 data points at once with a maximum of 1000 results(100 pages) we can iterate through using the 'page' parameter 
    # This makes it necessary to just query by date when we run out of pages, and update the latest date of article we have when querying next, until we have all the articles.
    # Note that this introduces the possibility of duplicates, which is why we maining a set of article ids to maintain data integrity
    # In order to adhere to NYTs API per minute/per day hit limit 5/min, 500/day, we have added necessary sleep functions. The daily limit is just handled by showing limit reached respinse from the AP
    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """
        # TODO: implement - this dummy implementation returns one batch of data
        current_date_lower_bound = '0000-01-01'
        page_num = 0
        article_id_set = set()
        q = deque() # Maintaining a queue so we can dump already yielded data from memory, can do with multiprocessing later.
        while True:
            filter_query = 'pub_date:>={}'.format(current_date_lower_bound)
            parameterised_api_url = self.args.generic_api_url + '?q={}&api-key={}&sort={}&fq={}&page={}'.format(self.args.query, 
                                                                                                self.args.APP_KEY, 
                                                                                                self.args.sort,
                                                                                                filter_query,
                                                                                                page_num)
            get_response = requests.get(parameterised_api_url)
            response_json = json.loads(get_response.text)

            if response_json.get('fault') is not None:
                print(response_json['fault'])
                time.sleep(12)
                continue
            if response_json['status'] == 'ERROR':
                print('\nGet request has returned an error, please check your query/url\n')
                break
            if response_json['response']['meta']['hits'] == 0:
                print('End of results')
                break
            
            for doc in response_json['response']['docs']:
                if doc['_id'] not in article_id_set:
                    article_id_set.add(doc['_id'])
                    q.append(self.flatten(doc))

            page_num += 1
            if page_num >= 100:
                current_date_lower_bound = response_json['response']['docs'][-1]['pub_date'][:10]
                page_num = 0
            
            if len(q) >= batch_size:
                yield (self.getn(q, batch_size))

            time.sleep(12) # Time required to not hit the per minute quota for the NYT API
            # yield [
            #     {
            #         "headline.main": "The main headline",
            #         "_id": "1234",
            #     }
            # ]

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        schema = [
            "title",
            "body",
            "created_at",
            "id",
            "summary",
            "abstract",
            "keywords",
        ]

        return schema


if __name__ == "__main__":
    # config = {
    #     "api_key": "NYTIMES_API_KEY",
    #     "query": "Silicon Valley",
    # }

    # Parser for getting API credentials from cfg file, removing API key from the code
    configParser = ConfigParser()
    configFilePath = 'nyt_article_search.cfg'

    with open(configFilePath) as f:
        configParser.read_file(f)

    config = dict()
    config['APP_KEY'] = configParser["credentials"]['APP_KEY']
    config['APP_SECRET'] = configParser['credentials']['APP_SECRET']
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config, query='Silicon Valley', 
                                     generic_api_url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json', 
                                     sort='oldest')
    for idx, batch in enumerate(source.getDataBatch(10)):
        print(f"{idx+1} Batch of {len(batch)} items")
        for item in batch:
            print(f"  - {item['_id']} - {item['headline.main']}")
