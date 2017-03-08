import argparse
import json
import logging
import os
from multiprocessing.dummy import Pool as ThreadPool
from os.path import dirname, join

from tqdm import tqdm

from api import Api
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

LOGIN = os.environ.get('LOGIN')
PASSWORD = os.environ.get('PASSWORD')
api = Api(LOGIN, PASSWORD)

parser = argparse.ArgumentParser(description='Scrape couchsurfing.com')
parser.add_argument('-s', '--start', help='Start from this id', required=True)
parser.add_argument('-c', '--count', help='Scrape this number ids', required=True)


DIR = 'data'  # dir to save results
POOL_SIZE = 16  # number of parallel processes

# TODO: seems like ugly way to catch multiprocess exceptions
def get_user(id):
    try:
        res = api.get_user(id)
        return res
    except Exception as e:
        logging.error(str(e))


if __name__ == '__main__':
    args = parser.parse_args()
    START = args.start
    COUNT = args.count
    END = START + COUNT
    logging.basicConfig(filename='{}-{}.log'.format(START, END), level=logging.ERROR)

    output = open('{}/users{}-{}.json'.format(DIR, START, END), 'w', buffering=10000)
    pool = ThreadPool(POOL_SIZE)
    ids = range(START, END)

    for user in tqdm(pool.imap_unordered(get_user, ids), mininterval=5):
        if user:
            # print(user['id'])
            output.write(json.dumps(user))
            output.write('\n')
            # output.flush()

    pool.close()
    pool.join()
