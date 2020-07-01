package de.robertmetzger;

import org.apache.flink.api.java.utils.ParameterTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.util.Calendar;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Table:
 *
 * create table test_results
 * (
 *     id       INTEGER
 *         constraint table_name_pk
 *             primary key autoincrement,
 *     name     TEXT    not null,
 *     duration INTEGER not null,
 *     "commit" TEXT,
 *     added_at DATE
 * );
 */
public class E2ELogAnalyzer {
    private final static Logger LOG = LoggerFactory.getLogger(LogsIndexer.class);
    private final ParameterTool parameters;

    private final Pattern BASH_PATTERN = Pattern.compile(".*] '([^']+)'.* ([0-9]+) minutes.* ([0-9]+) seconds!.*");
    private final Pattern JAVA_PATTERN = Pattern.compile(".*Time elapsed: ([0-9]+)(.[0-9]+)? s - in (.*)");
    private final Pattern COMMIT_PATTERN = Pattern.compile(".*git checkout --progress --force ([a-z0-9-_.]+).*");
    private final Connection connection;

    public E2ELogAnalyzer(ParameterTool parameters) throws SQLException {
        this.parameters = parameters;
        // create a database connection
        this.connection = DriverManager.getConnection(parameters.get("database", "jdbc:sqlite:/Users/robert/Projects/flink-workdir/flink-logs-indexer/e2e-data.sqlite"));
        connection.setAutoCommit(true);
    }

    private void run() throws FileNotFoundException, SQLException {
        LOG.info("Starting E2E Log Analyzer");

        File dataDir = new File(parameters.get("data-dir", "."));
        File[] logFiles = dataDir.listFiles((dir, name) -> name.startsWith("e2e-dataoutput"));
        if(logFiles == null) {
            LOG.warn("No logfiles found in " + dataDir.getAbsolutePath());
            return;
        }

        // process them one after another
        ExecutorService executorService = Executors.newFixedThreadPool(parameters.getInt("concurrent-processing",8));
        for(File logFile: logFiles) {
            if (logFile.isDirectory()) {
                LOG.debug("Skipping directory " + logFile);
                continue;
            }
            executorService.submit(() -> {
                try {
                    analyzeFile(logFile);
                } catch (Throwable throwables) {
                    LOG.warn("Error while processing file", throwables);
                }
            });
        }
    }

    private void analyzeFile(File logFile) throws SQLException, FileNotFoundException {
        LOG.info("Analyzing file {}", logFile);

        Pattern buildIdPattern = Pattern.compile(".*output-([0-9]+)-.*");
        Matcher buildIdMatcher = buildIdPattern.matcher(logFile.getName());
        if(!buildIdMatcher.find()) {
            throw new RuntimeException("Unexpected file name");
        }
        int buildId = Integer.parseInt(buildIdMatcher.group(1));

        Scanner scanner = new Scanner(new FileInputStream(logFile));
        scanner.useDelimiter("\n");
        String commitSha = "";
        String runDate = null;
        int results = 0;
        while(scanner.hasNext()) {
            String line = scanner.next();
            if(runDate == null) {
                // the first line looks like this
                // 2020-06-08T02:50:48.5046084Z ##[section]Starting: e2e_ci
                runDate = line.split("T")[0];
            }
            // find commit
            if(line.contains("git checkout --progress --force")) {
                Matcher commitMatch = COMMIT_PATTERN.matcher(line);
                if(!commitMatch.find()) {
                    throw new RuntimeException("Error. Unexpected format in line " + line);
                }
                commitSha = commitMatch.group(1);
            }

            Matcher javaMatcher = JAVA_PATTERN.matcher(line);
            if(javaMatcher.find()) {
                // we found a java e2e test
                emitTestResult(javaMatcher.group(3), Integer.parseInt(javaMatcher.group(1)), buildId, commitSha, runDate);
                results++;
            }

            Matcher bashMatcher = BASH_PATTERN.matcher(line.trim());
            if(bashMatcher.find()) {
                // found a bash e2e test
                int seconds = Integer.parseInt(bashMatcher.group(2)) * 60 + Integer.parseInt(bashMatcher.group(3));
                emitTestResult(bashMatcher.group(1), seconds, buildId, commitSha, runDate);
                results++;
            }
        }
        LOG.info("Done with file. Processed {} results", results);
    }

    private synchronized void emitTestResult(String testName, int seconds, int buildId, String commitSha, String runDate) throws SQLException {
        final String INSERT_PULL_REQUEST = "INSERT INTO \"test_results\" "
                + "(\"name\", \"build_id\", \"duration\", \"commit\", \"run_date\", \"added_at\") "
                + "VALUES (?, ?, ?, ?, ?, ?)";
        try(PreparedStatement statement = connection.prepareStatement(INSERT_PULL_REQUEST)) {
            statement.setString(1, testName);
            statement.setInt(2, buildId);
            statement.setInt(3, seconds);
            statement.setString(4, commitSha);
            statement.setString(5, runDate);
            statement.setDate(6, new Date(Calendar.getInstance().getTime().getTime()));
            statement.execute(); // insert :)
        }
    }

    public static void main(String[] args) throws FileNotFoundException, SQLException {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        E2ELogAnalyzer analyzer = new E2ELogAnalyzer(parameters);

        analyzer.run();
    }
}
