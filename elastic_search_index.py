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
f = open('pages.json')
nb_doc = 0
for doc in f:
    doc = json.loads(doc)
    del doc['_id']
    del doc['html']
    index_doc(doc)
    nb_doc += 1
    if nb_doc % 100 == 0:
        print('.', end='')
        sys.stdout.flush()
f.close()
print(f'\n{nb_doc} documents indexed...')
end_time = time.time()
print(f'Running time: {end_time - start_time} seconds...')