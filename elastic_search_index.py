import json, sys, codecs, time
from elasticsearch import Elasticsearch

def index_doc(doc):
    myid = doc['url']
    res = es.index(index=INDEX, doc_type=DOCTYPE, id=myid, body=doc)
    return True

start_time = time.time()

ES_HOST = 'http://localhost:9200'
INDEX ='moovle'; DOCTYPE = 'webpage'
es = Elasticsearch(ES_HOST)
f = codecs.open('pages.json', 'r','utf-8')
nb_doc = 0
line_count = 0
for doc in f:
    line_count += 1
    if doc.replace(' ', '').replace('\n', '') == '':
        continue
    try:
        doc = json.loads(doc)
        del doc['_id']
        del doc['html']
        index_doc(doc)
        nb_doc += 1
        if nb_doc % 100 == 0:
            print('.', end='')
            sys.stdout.flush()
    except Exception:
        with codecs.open('log.txt', 'w', 'utf-8') as f:
            f.write(f'error: line {line_count}\n')
f.close()
print(f'\n{nb_doc} documents indexed...')
end_time = time.time()
print(f'Running time: {end_time - start_time} seconds...')