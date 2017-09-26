# Description 

The application extracts entities and relations from [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) json dump file and sinks into Elasticsearch. Before using the application, please read the data model of Wikidata [here](https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON).

Wikidata dump file consists of *items* and *properties*. The application inserts all entities (including the properties) as *id + type+ label* . For example; entity's id is *Q64*, type is *item*, label is *"Berlin"*. Note that entities also have "aliases", for our research purposes we create new docs for labels and also aliases. For instance; item is "Berlin", English alias is "Berlin, Germany" so we have the docs below in Elasticsearch. 

| id        | type           | label  |
| ------------- |:-------------:| -----:|
| Q64      | item | Berlin|
| Q64      | item      |   Berlin, Germany|

Wikidata item has claim attributes, we simply treated claims as relations such that;
*Entity_id + Property_id + Object_id*

Entity is the item itself (i.e. Q64). Each claim of the entity is a relation (i.e. property of the claim P610). Objects are data values of the properties, it doesn't have to be another entity but all data values are considered as objects. Here for property P610 the object is Q19259618. You can check the items via appending to the url https://www.wikidata.org/wiki/?	like these examples : 

 - https://www.wikidata.org/wiki/Q64
 - https://www.wikidata.org/wiki/Property:P610
 - https://www.wikidata.org/wiki/Q19259618

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
 
#### **Important!** Before starting the process, be sure you have the dump file in your local file system. You can download it [here](https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2). The input path and the other settings (e.g the number of shards and replicas) are configured from **config.properties**
#### STARTING THE PROCESS

 - Prepare your package
	 - cd /.../wd-kb-extractor 	
	 - mvn package
		 - jar will be available under /target/wd-kb-extractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 - Start Elasticsearch
	- cd elasticsearch-2.2.1/
	- bin/elasticsearch -d (start as a daemon!)
 - Start the process via nohup command => nohup java -jar wd-kb-extractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
 - You can check the log file => less wd-kb-extractor.log
 - Start Kibana
	- cd kibana-4.4.0-linux-x64/
	- bin/kibana 
	- Go to http://localhost:5601/app/sense and run the queries.


Example queries;

    GET wikidata-index/wikidata-entity/_search
    { "size":20, 
	  "query": {
      "match": {
	      "id": "Q64"
	      } 
	   } 
	}

Returns the item Q64 including it's aliases.


----------


    GET wikidata-index/wikidata-claim/_search
    { "size":20,  
      "query": {
      "match": {
	      "entity_id": "Q64"
	      } 
	   } 
	}

Returns all relations of item Q64.
