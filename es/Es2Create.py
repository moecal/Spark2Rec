from elasticsearch import Elasticsearch

if __name__ == '__main__':
    es=Elasticsearch(hosts=['120.27.241.54:9200'])
    mappings={
        "mappings":{
            "testType":{
                "properties":{

                }
            }
        }

    }