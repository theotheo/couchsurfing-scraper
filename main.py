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
PASSWORD = os.environ('PASSWORD')
api = Api(LOGIN, PASSWORD)

START = 30000
COUNT = 120000
END = START + COUNT
POOL_SIZE = 16

logging.basicConfig(filename='{}-{}.log'.format(START, END), level=logging.ERROR)

if __name__ == '__main__':
    output = open('users{}-{}.json'.format(START, END), 'w', buffering=10000)
    pool = ThreadPool(POOL_SIZE)
    for group in tqdm(range(START, END, POOL_SIZE)):
        ids = list(range(group, group + POOL_SIZE))
        try:
            for user in pool.imap(api.get_user, ids):
                output.write(json.dumps(user))
                output.write('\n')
            # output.flush()
        except Exception as e:
            logging.error(str(e))

    pool.close()
    pool.join()
