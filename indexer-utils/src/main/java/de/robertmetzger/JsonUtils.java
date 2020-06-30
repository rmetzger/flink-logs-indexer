package de.robertmetzger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Cache;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.math3.analysis.function.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class JsonUtils {
    private final static Logger LOG = LoggerFactory.getLogger(JsonUtils.class);

    private final OkHttpClient client;
    private final ObjectMapper objectMapper;

    public JsonUtils() {
        client = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.MINUTES)
                .callTimeout(10, TimeUnit.MINUTES)
                .readTimeout(10, TimeUnit.MINUTES)
                .writeTimeout(10, TimeUnit.MINUTES)
                .cache(new Cache(new File("/tmp/okhttpcache"), 50* 1024*1024))
                .build();
        objectMapper = new ObjectMapper();
    }

    public static class JsonResult<T> {
        public T result;
        public Headers responseHeaders;
    }
    public <T> JsonResult<T> getJsonFromUrl(String url, Class<T> target) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .build();
        try (Response response = client.newCall(request).execute()) {
            String responseBody = response.body().string();
            JsonResult<T> res = new JsonResult<>();
            res.responseHeaders = response.headers();
            res.result = objectMapper.readValue(responseBody, target);
            return res;
        }
    }
}
