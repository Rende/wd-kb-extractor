
# Description 

The application extracts entities and relations from [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) json dump file and sinks into Elasticsearch (ES). Before using the application, please read the data model of Wikidata [here](https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON).

Wikidata dump file consists of *items* and *properties*. The application inserts all entities (including the properties) as *id , type, label, aliases, tokenized label, wikipedia-title, tokenized aliases, claims*. For tokenizing, JTok and Stanford Lemmatizer are applied. Wikipedia-title is also stored to relate the item with its wikipedia page (this field is available in the dump). Claims are list of (property, item) tuples which are used to extract the relations between items.


For example;  the entry *"Berlin"* has a document in ES as below:

    "_id": "Q64"
    "type": "item",
    "label": "Berlin",
    "tok-label": "Berlin",
    "wiki-title": "Berlin",
    "aliases": [
      "Berlin",
      "Berlin, Germany"
     ], 
     "tok-aliases": [
       "Berlin",
       "Berlin , Germany"
   ],
     "claims": [
        ...
          {
           "property-id": "P131",
              "object-id": "Q183"
            },
            ...
      ]

Claims are treated as relations (entity<sub>1</sub>, relation, entity<sub>2</sub>) such that;
*entity-id + property-id + object-id* 

Entity is the item itself (i.e. Q64). Each claim contains a relation i.e. property = relation, item = object-id
Here for property P131 (alias: is located in) the object is Q183 (Germany). 
The relation is (Berlin, is located in, Germany)

You can check the items via appending to the url https://www.wikidata.org/wiki/?  like these examples : 

 - https://www.wikidata.org/wiki/Q64
 - https://www.wikidata.org/wiki/Property:P131
 - https://www.wikidata.org/wiki/Q183

We only take English attributes into consideration, still working on making it multilingual. 

# Requirements
Application inserts documents into Elasticsearch therefore you should have a running Elasticsearch instance on your machine. 

download elasticsearch-2.2.1

    - wget "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.2.1/elasticsearch-2.2.1.tar.gz"
  - tar -xvf elasticsearch-2.2.1.tar.gz

edit config file

    - cd elasticsearch-2.2.1/config/
  - nano elasticsearch.yml
    set cluster name => cluster.name: wd-kbe-cluster
    set (optional) data path => path.data: /.../es-data-2.2.1 (if you don't, it'll create dirs under elasticsearch-2.2.1)
    set (optional) log path => path.logs: /.../es-log-2.2.1
    save and exit

Elasticsearch can be used with some other plugins like Kibana and Sense for dev console (for running queries).

download kibana 4.4.0 (compatible version of elasticsearch-2.2.1)

    - wget "https://download.elastic.co/kibana/kibana/kibana-4.4.0-linux-x64.tar.gz"
  - tar -xvf kibana-4.4.0-linux-x64.tar.gz
    - cd kibana-4.4.0-linux-x64/
  - bin/kibana plugin --install elastic/sense
    - bin/kibana
 
 For tokenizing, JTok should be installed, it can be found [here](https://github.com/DFKI-MLT/JTok)
 For lemmatizing, Stanford CoreNLP model jar has to be downloaded [here](http://nlp.stanford.edu/software/stanford-english-corenlp-2017-06-09-models.jar), after downloading, copy the file under "src/main/resources" folder then create a new folder named "local-repo" under your project folder then run the command below:

    mvn install:install-file -Dfile=src/main/resources/stanford-english-corenlp-2017-06-09-models.jar -DgroupId=edu.stanford.nlp -DartifactId=stanford-english-corenlp-2017-06-09-models -Dversion=1.0 -Dpackaging=jar -DlocalRepositoryPath=local-repo

 
 
**Important!** Before starting the process, be sure you have the dump file in your local file system. You can download it [here](https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2). The input path and the other settings (e.g the number of shards and replicas) are configured from **config.properties**
#### STARTING THE PROCESS

 - Prepare your package
   - cd /.../wd-kb-extractor  
   - mvn package
     - jar will be available under /target/wd-kb-extractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 - Start Elasticsearch
  - cd elasticsearch-2.2.1/
  - bin/elasticsearch -d (start as a daemon!)
 - Start the process via nohup command => nohup java -jar wd-kb-extractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar &
 - You can check the log file => less wd-kb-extractor.log
 - Start Kibana
  - cd kibana-4.4.0-linux-x64/
  - bin/kibana 
  - Go to http://localhost:5601/app/sense and run the queries.


Example queries;

    GET wikidata-index/wikidata-entities/_search
    { "size":20, 
    "query": {
      "match": {
        "id": "Q64"
        } 
     } 
  }

Returns the item Q64 as a document.

