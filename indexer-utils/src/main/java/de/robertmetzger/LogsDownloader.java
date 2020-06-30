package de.robertmetzger;

import org.apache.flink.api.java.utils.ParameterTool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Azure REST API:
 *
 *  List of builds:
 *  https://dev.azure.com/apache-flink/apache-flink/_apis/build/builds?api-version=5.1
 *
 *  Get artifacts of a build:
 *  https://dev.azure.com/apache-flink/apache-flink/_apis/build/builds/{buildId}/artifacts?api-version=5.1
 */
public class LogsDownloader {
    private final static Logger LOG = LoggerFactory.getLogger(LogsDownloader.class);

    private final JsonUtils jsonUtils;
    private final ParameterTool parameters;
    private final String azureOrg;


    public LogsDownloader(ParameterTool pt) {
        jsonUtils = new JsonUtils();
        this.parameters = pt;
        this.azureOrg = parameters.get("azure-org", "apache-flink/apache-flink");
    }

    private List<Integer> getBuildIDs() throws IOException {
        List<Integer> buildIds = new ArrayList<>();
        List<Integer> fetchResult;
        String continuationToken = "";
        do {
            JsonUtils.JsonResult<ObjectNode> callResult =
                    jsonUtils.getJsonFromUrl("https://dev.azure.com/" + azureOrg + "/_apis/build/builds?api-version=5.1&continuationToken="+continuationToken+"&$top=2500", ObjectNode.class);
            // LOG.debug("res = " + callResult.result);
            JsonNode buildsArray = callResult.result.get("value");
            if(buildsArray == null) {
                LOG.debug("Unexpected REST result " + callResult.result);
                return Collections.emptyList();
            }
            int count = callResult.result.get("count").asInt();
            continuationToken = callResult.responseHeaders.get("x-ms-continuationtoken");
            LOG.debug("Fetch count {}. Continuation token {}", count, continuationToken);

            LOG.info("size = " + buildsArray.size());
            fetchResult = StreamSupport.stream(buildsArray.spliterator(), false)
                    // only on master pushes
                    .filter(node -> {
                        JsonNode triggerInfo = node.get("triggerInfo");
                        if(!triggerInfo.has("ci.triggerRepository")) {
                            return false;
                        }
                        String repo = triggerInfo.get("ci.triggerRepository").asText();
                        return repo.equals("flink-ci/flink-mirror") || repo.equals("flink-ci/flink");
                    })
                    .map(node -> node.get("id").asInt())
                    .collect(Collectors.toList());
            buildIds.addAll(fetchResult);
            LOG.info("Fetched {} build IDs", fetchResult.size());
            LOG.debug("fetched tokens: " + fetchResult);
            if(continuationToken == null) {
                break;
            }
        } while(fetchResult.size() > 0);
        LOG.info("Finished fetching. Total number of IDs fetched {}", buildIds.size());
        return buildIds;
    }

    private Stream<Download> getLogDownloadLinks(int buildID) throws IOException {
        ObjectNode artifactsResultObject = jsonUtils.getJsonFromUrl("https://dev.azure.com/" + azureOrg + " /_apis/build/builds/" + buildID + "/artifacts?api-version=5.1", ObjectNode.class).result;
        JsonNode artifactsArray = artifactsResultObject.get("value");
        return StreamSupport.stream(artifactsArray.spliterator(), false)
                .filter(artifact -> artifact.get("name").asText().startsWith("logs-"))
                .map(artifact -> {
                    Download dl = new Download();
                    dl.url = artifact.get("resource").get("downloadUrl").asText();
                    dl.name = buildID + "-" + artifact.get("name").asText();
                    return dl;
                });
    }

    @FunctionalInterface
    public interface ThrowingSupplier<T, E extends Exception> {
        T get() throws E;
    }

    public static class Download {
        public String url;
        public String name;

        @Override
        public String toString() {
            return "Download{" +
                    "url='" + url + '\'' +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    private static <R> Stream<R> emptyStreamOnException(ThrowingSupplier<Stream<R>, IOException> fun) {
        try {
            return fun.get();
        } catch (IOException e) {
            LOG.debug("Exception while retrieving download links", e);
            return Stream.empty();
        }
    }

    private void run() throws IOException, InterruptedException {
        LOG.info("Getting builds from Azure");
        // ObjectNode builds = jsonUtils.getObjectNodeFromUrl("https://dev.azure.com/apache-flink/apache-flink/_apis/build/builds?api-version=5.1");
        List<Integer> buildIDs = getBuildIDs();
        LOG.info("ids = " + buildIDs);

        System.exit(0);
        LOG.info("Collecting download links for builds");
        List<Download> downloadLinks = buildIDs
                .stream()
                .flatMap(buildID -> emptyStreamOnException(() -> getLogDownloadLinks(buildID)))
                .collect(Collectors.toList());

        ExecutorService executorService = Executors.newFixedThreadPool(parameters.getInt("concurrent-downloads",4));
        for(Download download: downloadLinks) {
            executorService.submit( () -> {
                try {
                    LOG.info("Downloading " + download);
                    File target = new File(parameters.get("data-dir", "data/") + download.name);
                    if(target.exists()) {
                        LOG.info("File {} exists already. Skipping download ...", target);
                        return;
                    }
                    FileUtils.copyURLToFile(new URL(download.url), target);
                } catch(Throwable t) {
                    LOG.info("Exception while downloading", t);
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }



    public static void main(String[] args) throws IOException, InterruptedException {
        ParameterTool pt = ParameterTool.fromArgs(args);

        LogsDownloader ld = new LogsDownloader(pt);
        ld.run();
    }
}
