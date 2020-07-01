package de.robertmetzger;

import org.apache.flink.api.java.utils.ParameterTool;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class E2EReportGenerator {
    private final ParameterTool parameters;
    private final QueryRunner run = new QueryRunner();
    private final MapListHandler mapListHandler = new MapListHandler();
    private final Connection connection;

    private final String BUILD_DETAIL_PAGE = "https://dev.azure.com/apache-flink/apache-flink/_build/results?buildId=";

    public E2EReportGenerator(ParameterTool parameters) throws SQLException {
        this.parameters = parameters;

        // Set up db connection
        this.connection = DriverManager.getConnection(parameters.get("database", "jdbc:sqlite:/Users/robert/Projects/flink-workdir/flink-logs-indexer/e2e-data.sqlite"));
        connection.createStatement().execute("PRAGMA cache_size = 10000");
    }

    public void run() throws IOException, SQLException, TemplateException {
        /* Create and adjust the configuration singleton */
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_27);
        //cfg.setDirectoryForTemplateLoading(new File("/where/you/store/templates"));
        cfg.setClassForTemplateLoading(E2EReportGenerator.class, "/");

        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setLogTemplateExceptions(false);
        cfg.setWrapUncheckedExceptions(true);

        /* Create a data-model */
        Map<String, Object> root = new HashMap<>();
        root.put("aggTable", computeAggregation());
        root.put("perTestStats", computePerTestStats());
        root.put("generatedTime", new Date().toString());

        System.out.println("root = " + root);

        Template temp = cfg.getTemplate("report.ftlh");

        /* Merge data-model with template */
        Writer out = new OutputStreamWriter(new FileOutputStream("www/index.html"));
        temp.process(root, out);
        out.close();
    }


    private List<String> getTestNames() throws SQLException {
        ColumnListHandler<String> columnListHandler = new ColumnListHandler<>();
        return run.query(connection, "SELECT DISTINCT name FROM test_results", columnListHandler);
    }
    private Map<String, TestStats> computePerTestStats() throws SQLException {
        List<String> testNames = getTestNames();
        Map<String, TestStats> result = new HashMap<>();
        for(String test: testNames) {
            result.put(test, computeTestStats(test));
        }
        return result;
    }

    private TestStats computeTestStats(String test) throws SQLException {
        TestStats result = new TestStats();
        // slow tests
        List<Map<String, Object>> slowTestsQueryResult = run.query(connection, "SELECT build_id, duration " +
                "FROM test_results " +
                "WHERE name = ? " +
                "ORDER BY duration DESC " +
                "LIMIT 10", mapListHandler, test);

        result.slowTests = slowTestsQueryResult.stream().map(res -> {
            ReportUrl url = new ReportUrl();
            Object id = res.get("build_id");
            url.url = BUILD_DETAIL_PAGE + id;
            url.name = "Build #" + id + " (" + res.get("duration") + " seconds)";
            return url;
        }).collect(Collectors.toList());

        // last runs
        List<Map<String, Object>> lastRunsQueryResult = run.query(connection, "SELECT build_id, duration, run_date " +
                "FROM test_results " +
                "WHERE name = ? " +
                "ORDER BY run_date DESC " +
                "LIMIT 10", mapListHandler, test);

        result.lastExecutions = lastRunsQueryResult.stream().map(res -> {
            ReportUrl url = new ReportUrl();
            Object id = res.get("build_id");
            url.url = BUILD_DETAIL_PAGE + id;
            url.name = "Build #" + id + " (Executed on " + res.get("run_date") + ") (" + res.get("duration") + "s)";
            return url;
        }).collect(Collectors.toList());

        return result;
    }

    private List<Map<String, Object>> computeAggregation() throws SQLException {
        return run.query(connection,
                    "SELECT name, AVG(duration) AS avg, MIN(duration) AS min, MAX(duration) AS max, COUNT(*) AS runs " +
                        "FROM test_results " +
                        "GROUP BY name " +
                        "ORDER BY name", mapListHandler);
    }

    public static class TestStats {
        public List<ReportUrl> slowTests = new ArrayList<>();
        public List<ReportUrl> lastExecutions = new ArrayList<>();

        public List<ReportUrl> getSlowTests() {
            return slowTests;
        }

        public List<ReportUrl> getLastExecutions() {
            return lastExecutions;
        }
    }

    public static class ReportUrl {
        public String name;
        public String url;

        public String getName() {
            return name;
        }

        public String getUrl() {
            return url;
        }
    }

    public static void main(String[] args) throws IOException, TemplateException, SQLException {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        E2EReportGenerator gen = new E2EReportGenerator(parameters);
        gen.run();
    }


}
