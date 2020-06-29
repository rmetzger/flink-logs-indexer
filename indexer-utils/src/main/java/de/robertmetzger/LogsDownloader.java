package de.robertmetzger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
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

    public LogsDownloader() {
        jsonUtils = new JsonUtils();
    }

    // TODO: retrieve all builds, not just the first 1000
    private List<Integer> getBuildIDs() throws IOException {
        ObjectNode buildsResultObject = jsonUtils.getJsonFromUrl("http://localhost:8000/builds-list-full.json", ObjectNode.class);
        JsonNode buildsArray = buildsResultObject.get("value");
        return StreamSupport.stream(buildsArray.spliterator(), false)
                .map(node -> node.get("id").asInt())
                .limit(5)
                .collect(Collectors.toList());
    }

    private Stream<Download> getLogDownloadLinks(int buildID) throws IOException {
        ObjectNode artifactsResultObject = jsonUtils.getJsonFromUrl("https://dev.azure.com/apache-flink/apache-flink/_apis/build/builds/" + buildID + "/artifacts?api-version=5.1", ObjectNode.class);
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

    private void run() throws IOException {
        LOG.info("Getting builds from Azure");
        // ObjectNode builds = jsonUtils.getObjectNodeFromUrl("https://dev.azure.com/apache-flink/apache-flink/_apis/build/builds?api-version=5.1");
        List<Integer> buildIDs = getBuildIDs();
        System.out.println("ids = " + buildIDs);

        List<Download> downloadLinks = buildIDs
                .stream()
                .flatMap(buildID -> emptyStreamOnException(() -> getLogDownloadLinks(buildID)))
                .collect(Collectors.toList());
        System.out.println("downloads : " + downloadLinks);
        for(Download download: downloadLinks) {
            LOG.info("Downloading " + download);
            FileUtils.copyURLToFile(
                    new URL(download.url),
                    new File(download.name));
        }
    }



    public static void main(String[] args) throws IOException {
        LogsDownloader ld = new LogsDownloader();
        ld.run();
    }
}
