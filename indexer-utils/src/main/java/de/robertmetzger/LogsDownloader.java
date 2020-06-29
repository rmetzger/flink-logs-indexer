package de.robertmetzger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
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
    public static void main(String[] args) throws IOException {
        JsonUtils jsonUtils = new JsonUtils();
        System.out.println("Getting builds from Azure");
        // ObjectNode builds = jsonUtils.getObjectNodeFromUrl("https://dev.azure.com/apache-flink/apache-flink/_apis/build/builds?api-version=5.1");
        ObjectNode buildsResultObject = jsonUtils.getJsonFromUrl("http://localhost:8000/builds-list-full.json", ObjectNode.class);
        JsonNode buildsArray = buildsResultObject.get("value");
        List<Integer> buildIds = StreamSupport.stream(buildsArray.spliterator(), false)
                .map(node -> node.get("id").asInt())
                .collect(Collectors.toList());

        System.out.println("ids = " + buildIds);
    }
}
