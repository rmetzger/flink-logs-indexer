package de.robertmetzger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Cache;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.File;
import java.io.IOException;

public class JsonUtils {

    private final OkHttpClient client;
    private final ObjectMapper objectMapper;

    public JsonUtils() {
        client = new OkHttpClient.Builder()
                .cache(new Cache(new File("/tmp/okhttpcache"), 50* 1024*1024))
                .build();
        objectMapper = new ObjectMapper();
    }

    public <T> T getJsonFromUrl(String url, Class<T> target) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .build();
        try (Response response = client.newCall(request).execute()) {
            String responseBody = response.body().string();
            return objectMapper.readValue(responseBody, target);
        }
    }
}
