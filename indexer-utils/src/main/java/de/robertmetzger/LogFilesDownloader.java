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
public class LogFilesDownloader {
    private final static Logger LOG = LoggerFactory.getLogger(LogFilesDownloader.class);

    private final JsonUtils jsonUtils;
    private final ParameterTool parameters;
    private final String azureOrg;


    public LogFilesDownloader(ParameterTool pt) {
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
                        JsonNode definition = node.get("definition");

                        String definitionName = definition.get("name").asText();
                        return definitionName.equals("flink-ci.flink-master-mirror") || definitionName.equals("flink-ci.flink");
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


    private void runGetLogOutput() throws IOException, InterruptedException {
        LOG.info("Getting builds from Azure");
        List<Integer> buildIDs = getBuildIDs();
        //List<Integer> buildIDs = List.of(4143, 4142, 4141);
        LOG.info("ids = " + buildIDs);
        LOG.info("Downloading logs");
        List<Download> urls = new ArrayList<>();
        for(Integer id: buildIDs) {
            // get download links
            List<Download> buildUrls = getE2ELogOutputLinks(id);
            LOG.info("Collected {} download URls for build #{}", buildUrls.size(), id);
            urls.addAll(buildUrls);
        }
        download(urls);
    }

    private List<Download> getE2ELogOutputLinks(Integer buildId) throws IOException {
        ObjectNode artifactsResultObject = jsonUtils.getJsonFromUrl("https://dev.azure.com/apache-flink/98463496-1af2-4620-8eab-a2ecc1a2e6fe/_apis/build/builds/"+buildId+"/Timeline", ObjectNode.class).result;
        JsonNode records = artifactsResultObject.get("records");
        List<Download> urls = new ArrayList<>();
        for(JsonNode record: records) {
            String name = record.get("name").asText();
            if(name.contains("e2e") && record.get("type").asText().equals("Job")) {
                if(record.get("log") != null && record.get("log").isObject()) {
                    Download dl = new Download();
                    dl.url = record.get("log").get("url").asText();
                    dl.name = "output-" + buildId + "-" + name;
                    urls.add(dl);
                }
            }
        }

        return urls;
    }


    private void download(List<Download> downloads) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(parameters.getInt("concurrent-downloads",4));
        for(Download download: downloads) {
            executorService.submit( () -> {
                try {
                    LOG.info("Downloading " + download);
                    File target = new File(parameters.get("data-dir", "data") + "/" + download.name);
                    if(target.exists()) {
                        LOG.info("File {} exists already. Skipping download ...", target);
                        return;
                    }
                    FileUtils.copyURLToFile(new URL(download.url), target);
                } catch(Throwable t) {
                    LOG.info("Exception while downloading name=" + download.name, t);
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.HOURS);
    }

    private void runGetLogFiles() throws IOException, InterruptedException {
        LOG.info("Getting builds from Azure");
        List<Integer> buildIDs = getBuildIDs();
        LOG.info("ids = " + buildIDs);

        LOG.info("Collecting download links for builds");
        List<Download> downloadLinks = buildIDs
                .stream()
                .flatMap(buildID -> emptyStreamOnException(() -> getLogDownloadLinks(buildID)))
                .collect(Collectors.toList());

        download(downloadLinks);
    }



    public static void main(String[] args) throws IOException, InterruptedException {
        ParameterTool pt = ParameterTool.fromArgs(args);

        LogFilesDownloader ld = new LogFilesDownloader(pt);
        if(pt.has("get-files")) {
            ld.runGetLogFiles();
        }
        if(pt.has("get-output")) {
            ld.runGetLogOutput();
        }

    }
}
