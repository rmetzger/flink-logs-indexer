package de.robertmetzger;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Utility that reads a Zipped file from Azure (containing a tgz file), and sends it to ES
 */
public class LogsIndexer {
    private final static Logger LOG = LoggerFactory.getLogger(LogsIndexer.class);
    private final BulkProcessor bulkProcessor;

    public LogsIndexer() {

       RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  Throwable failure) {
                LOG.info("ES error", failure);
                System.exit(1);
            }
        };

        BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener);
        builder.setBulkActions(5000);
        builder.setBulkSize(new ByteSizeValue(10L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(4);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        builder.setBackoffPolicy(BackoffPolicy
                .constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        this.bulkProcessor = builder.build();
    }

    private void run() throws IOException {
        File dataDir = new File(".");
        // get all log files in the data dir
        File[] logFiles = dataDir.listFiles((dir, name) -> name.contains("logs"));
        // process them one after another
        for(File logFile: logFiles) {
            if(logFile.isDirectory()) {
                LOG.debug("Skipping directory " + logFile);
                continue;
            }

            // they are zip files
            try(ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(logFile))) {
                LOG.info("Processing " + logFile);
                ZipEntry entry;
                while((entry = zipInputStream.getNextEntry()) != null) {
                    String n = entry.getName();
                    if(n.endsWith(".tgz") || n.endsWith(".tar.gz")) {
                        LOG.info("Uncompressing " + n);
                        InputStream gzi = new GzipCompressorInputStream(zipInputStream, false);
                        ArchiveInputStream tgzInput = new TarArchiveInputStream(gzi);
                        ArchiveEntry tgzEntry;
                        while ((tgzEntry = tgzInput.getNextEntry()) != null) {
                            LOG.info("tgz entry: " + tgzEntry.getName());
                            if (tgzEntry.getName().endsWith(".log")) {
                                parseLogfile(tgzInput, logFile.getName() + "-" + tgzEntry.getName(), n);
                            }
                        }
                        zipInputStream.closeEntry();
                    } else {
                        LOG.warn("Unexpected ZIP file content: "+ entry);
                    }
                }
            }
        }
        LOG.info("Done processing the files ...");
        bulkProcessor.close();
    }

    /**
     * Parse log file
     * @param logStream
     */
    private void parseLogfile(InputStream logStream, String buildname, String innerArchiveName) throws IOException {
        Instant tsInst;
        // this parses the "old"? format, such as: "logs-ci-blinkplanner/20200629.4.tar.gz" or "logs-ci-e2e/20200629.4.tgz"
        if(innerArchiveName.contains("/20")) { // this will stop working in the year 2100+
            Pattern datePattern = Pattern.compile(".*/(20[0-9]+)\\..*");
            Matcher m = datePattern.matcher(innerArchiveName);
            if(!m.find()) {
                throw new RuntimeException("Expected to find date in inner archive name " + innerArchiveName);
            }
            SimpleDateFormat timePattern = new SimpleDateFormat("yyyyMMdd");
            try {
                tsInst = timePattern.parse(m.group(1)).toInstant();
            } catch (ParseException e) {
                throw new RuntimeException("unexpected");
            }
        } else {
            Pattern timestampPattern = Pattern.compile(".*-([0-9]{10}).*");
            Matcher m = timestampPattern.matcher(buildname);
            if (!m.find()) {
                throw new RuntimeException("Error: expected to find timestamp in " + buildname);
            }
            int baseTs = Integer.parseInt(m.group(1));
            tsInst = Instant.ofEpochSecond(baseTs).minus(8, ChronoUnit.HOURS);
        }
        LOG.info("parsed " + tsInst);
        Instant baseTsBeginningOfDay = tsInst.truncatedTo(ChronoUnit.SECONDS).truncatedTo(ChronoUnit.MINUTES).truncatedTo(ChronoUnit.HOURS);
        Instant logEventTime = tsInst;
        LOG.info("begin of day " + tsInst);

        Pattern logTimestampPattern = Pattern.compile("ˆ([0-9:,]+) \\[.*");
        SimpleDateFormat timePattern = new SimpleDateFormat("HH:mm:ss,SSS");

        BufferedReader br = new BufferedReader(new InputStreamReader(logStream));
        String line;
        while ((line = br.readLine()) != null) {
            Matcher timeMatcher = logTimestampPattern.matcher(line);
            if(timeMatcher.find()) {
                Instant time = null;
                String timeString = timeMatcher.group(1);
                try {
                    time = timePattern.parse(timeString).toInstant();
                    logEventTime = baseTsBeginningOfDay.plusMillis(time.toEpochMilli());
                } catch (ParseException e) {
                    LOG.debug("Error parsing date from log line '{}' ", line, e);
                }
            }
            emitLogToElastic(line, buildname, logEventTime.toEpochMilli());
        }
    }

    private void emitLogToElastic(String line, String buildname, long timestamp) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("buildname", buildname);
            builder.field("line", line);
            builder.timeField("log-ts", timestamp);
        }
        builder.endObject();
        bulkProcessor.add(new IndexRequest("logs").source(builder));
    }

    public static void main(String[] args) throws IOException, ArchiveException {
        LogsIndexer li = new LogsIndexer();
        li.run();
    }
}
