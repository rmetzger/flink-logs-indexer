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
mvn exec:java -Dexec.mainClass="de.robertmetzger.LogsDownloader"

# index files
mvn exec:java -Dexec.mainClass="de.robertmetzger.LogsIndexer"


# Start ES:
ES_JAVA_OPTS="-Xms4g -Xmx4g" ./bin/elasticsearch -d

# start Kibana:
./bin/kibana serve -e http://localhost:9200

# Launch local ELK
ocker run -p 5601:5601 -p 9200:9200 -p 5044:5044 -it --name elk -e LOGSTASH_START=0 -e KIBANA_START=1 -e MAX_MAP_COUNT=362144 sebp/elk
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