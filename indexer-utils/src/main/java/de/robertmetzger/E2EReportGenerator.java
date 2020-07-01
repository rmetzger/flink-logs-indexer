package de.robertmetzger;

import org.apache.flink.api.java.utils.ParameterTool;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class E2EReportGenerator {
    public static void main(String[] args) throws IOException, TemplateException, SQLException {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        /* Create and adjust the configuration singleton */
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_27);
        //cfg.setDirectoryForTemplateLoading(new File("/where/you/store/templates"));
        cfg.setClassForTemplateLoading(E2EReportGenerator.class, "/");

        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setLogTemplateExceptions(false);
        cfg.setWrapUncheckedExceptions(true);

        // Set up db connection
        Connection connection = DriverManager.getConnection(parameters.get("database", "jdbc:sqlite:/Users/robert/Projects/flink-workdir/flink-logs-indexer/e2e-data.sqlite"));
        //connection.createStatement().execute("PRAGMA cache_size = 10000");


        /* Create a data-model */
        Map<String, Object> root = new HashMap<>();
        root.put("agg-table", computeAggregation(connection));
        root.put("generatedTime", new Date().toString());

        System.out.println("root = " + root);

        Template temp = cfg.getTemplate("report.ftlh");

        /* Merge data-model with template */
        Writer out = new OutputStreamWriter(new FileOutputStream("www/index.html"));
        temp.process(root, out);
        out.close();
    }

    private static List<Map<String, Object>> computeAggregation(Connection connection) throws SQLException {
        QueryRunner run = new QueryRunner();
        MapListHandler resultSetHandler = new MapListHandler();
        return run.query(connection,
                    "SELECT name, AVG(duration), MIN(duration), MAX(duration), COUNT(*) " +
                        "FROM test_results " +
                        "GROUP BY name", resultSetHandler);
    }

}
