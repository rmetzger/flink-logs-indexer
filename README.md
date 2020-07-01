# flink-logs-indexer

Components:
- (planned) logs downloader: downloads Flink CI logs from an AZP pipeline
- (planned) logs indexer: reads logs into ElasticSearch
- (idea) logs receiver: accepts logs uploaded via HTTP
- (planned) search UI: search in ES
- 

## Operations

```bash
# download files
mvn exec:java -Dexec.mainClass="de.robertmetzger.LogFilesDownloader"

# index files
mvn exec:java -Dexec.mainClass="de.robertmetzger.LogsIndexer"

# index with custom properties
mvn exec:java -Dexec.mainClass="de.robertmetzger.LogsIndexer" -Dexec.args="--es.bulkactions 10000 --es.concurrent 10"

# Start ES:
ES_JAVA_OPTS="-Xms4g -Xmx4g" ./bin/elasticsearch -d

# start Kibana:
./bin/kibana serve -e http://localhost:9200

# Launch local ELK
docker run -p 5601:5601 -p 9200:9200 -p 5044:5044 -it --name elk -e LOGSTASH_START=0 -e KIBANA_START=1 -e MAX_MAP_COUNT=362144 sebp/elk

# access kibana:
http://localhost:5601/app/kibana#/discover?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-30M,to:now))&_a=(columns:!(_source),filters:!(),index:'2f3a1170-baf5-11ea-9c0a-2d4a12e98bbf',interval:auto,query:(language:kuery,query:''),sort:!())
```

Space estimation
```
find . -exec unzip {} \;

for f in logs*/*.tgz ; do echo "unzipping $f"; cd `dirname $f`; tar vxf `basename $f` ; cd /home/robert/flink-logs-indexer/indexer-utils/1gb ;  done
```


## Performance

On 2019 MacBook Pro (ES in Docker, default settings)
```
-- Meters ----------------------------------------------------------------------
files-processed
             count = 3
         mean rate = 0,02 events/second
     1-minute rate = 0,03 events/second
     5-minute rate = 0,01 events/second
    15-minute rate = 0,00 events/second
logs-indexed
             count = 1858933
         mean rate = 15414,66 events/second
     1-minute rate = 13443,90 events/second
```

On GCP n1-standard-4 (4 vCPUs, 15 GB memory)
```
-- Meters ----------------------------------------------------------------------
files-processed
             count = 2366
         mean rate = 0.06 events/second
     1-minute rate = 0.07 events/second
     5-minute rate = 0.08 events/second
    15-minute rate = 0.07 events/second
logs-indexed
             count = 846356213
         mean rate = 21518.97 events/second
     1-minute rate = 21981.14 events/second
     5-minute rate = 22027.31 events/second
    15-minute rate = 21594.48 events/second
```