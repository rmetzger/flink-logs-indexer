# flink-logs-indexer

Components:
- (planned) logs downloader: downloads Flink CI logs from an AZP pipeline
- (planned) logs indexer: reads logs into ElasticSearch
- (idea) logs receiver: accepts logs uploaded via HTTP
- (planned) search UI: search in ES
- 

## Operations

```bash
#download files
mvn exec:java -Dexec.mainClass="de.robertmetzger.LogsDownloader"

# Start ES:
ES_JAVA_OPTS="-Xms4g -Xmx4g" ./bin/elasticsearch -d

# start Kibana:
./bin/kibana serve -e http://localhost:9200
```