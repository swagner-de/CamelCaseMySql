import logging
import re
from DatabaseAgent import DatabaseAgent
from settings import *

def camel_casify(st):
    return st.title().replace(' ',''
)
def clean_string(st):
    return re.sub('[^a-zA-Z0-9-_*. ]', '',st)


def main():
    LOGLEVEL = 'DEBUG'

    logger = logging.getLogger()
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
    logger.setLevel(LOGLEVEL)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    da = DatabaseAgent(**dbconfig)

    query = 'SELECT id, %s FROM %s' % (source_column, table)

    result = da.query(query)

    for item in result:
        clean = clean_string(item['name'])
        camel_case = camel_casify(clean)
        persist_statement = "UPDATE %s SET %s = '%s' WHERE id = %s" % (table, target_column, camel_case, item['id'])
        da.execute(persist_statement)

    da.close()

if __name__ == '__main__':
    main()