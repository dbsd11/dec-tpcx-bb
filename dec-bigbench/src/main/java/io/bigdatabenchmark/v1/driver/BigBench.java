package io.bigdatabenchmark.v1.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class BigBench
        implements Runnable {
    private final List<Integer> BIG_BENCH_DEFAULT_QUERIES = new ArrayList(
            Arrays.asList(new Integer[]{Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3), Integer.valueOf(4), Integer.valueOf(5), Integer.valueOf(6), Integer.valueOf(7), Integer.valueOf(8), Integer.valueOf(9), Integer.valueOf(10), Integer.valueOf(11), Integer.valueOf(12), Integer.valueOf(13), Integer.valueOf(14), Integer.valueOf(15), Integer.valueOf(16), Integer.valueOf(17), Integer.valueOf(18), Integer.valueOf(19), Integer.valueOf(20), Integer.valueOf(21), Integer.valueOf(22), Integer.valueOf(23), Integer.valueOf(24), Integer.valueOf(25), Integer.valueOf(26), Integer.valueOf(27), Integer.valueOf(28), Integer.valueOf(29), Integer.valueOf(30)}));
    private final String[] BIG_BENCH_DEFAULT_QUERIES_TYPE = {"structured", "semi-structured",
            "semi-structured", "semi-structured", "semi-structured", "structured", "structured", "semi-structured",
            "structured", "unstructured", "structured", "semi-structured", "structured", "structured", "structured",
            "structured", "structured", "unstructured", "unstructured", "structured", "structured", "structured",
            "structured", "structured", "structured", "structured", "unstructured", "unstructured", "structured",
            "semi-structured"};

    private Logger log;

    private List<BenchmarkPhase> benchmarkPhases;

    private BigBenchRandom rnd;

    private ExecutorService executor;

    private LogTree logTreeRoot;

    private Map<String, List<Integer>> queryListCache;

    private String runScript;

    private Properties properties;

    private String version;

    private String homeDir;
    private String confDir;
    private String datagenDir;
    private String logDir;
    private String dataGenLogFile;
    private String loadLogFile;
    private String engine;
    private String database;
    private String mapTasks;
    private int numberOfParallelStreams;
    private long scaleFactor;
    private boolean pretendMode;
    private boolean printStdOut;
    private boolean stopAfterFailure;
    private List<String> userArguments;
    private long logTreeIDForCSVPrint;
    AtomicBoolean executorCleanedUp = new AtomicBoolean();

    private static enum BenchmarkPhase {
        BENCHMARK(
                null, "benchmark", false, false, false, false, "BigBench benchmark"),

        BENCHMARK_START(null, "benchmark_start", false, false, false, false, "BigBench benchmark: Start"),

        BENCHMARK_STOP(null, "benchmark_stop", false, false, false, false, "BigBench benchmark: Stop"),

        CLEAN_ALL("cleanAll", "clean_all", false, false, false, false, "BigBench clean all"),

        ENGINE_VALIDATION_CLEAN_POWER_TEST("cleanQuery", "engine_validation_power_test", false, false, false, false,
                "BigBench engine validation: Clean power test queries"),

        ENGINE_VALIDATION_CLEAN_LOAD_TEST("cleanMetastore", "engine_validation_metastore", false, false, false, false,
                "BigBench engine validation: Clean metastore"),

        ENGINE_VALIDATION_CLEAN_DATA("cleanData", "engine_validation_data", false, false, false, false,
                "BigBench engine validation: Clean data"),

        ENGINE_VALIDATION_DATA_GENERATION("dataGen", "engine_validation_data", false, false, false, true,
                "BigBench engine validation: Data generation"),

        ENGINE_VALIDATION_LOAD_TEST("populateMetastore", "engine_validation_metastore", false, false, false, true,
                "BigBench engine validation: Populate metastore"),

        ENGINE_VALIDATION_POWER_TEST("runQuery", "engine_validation_power_test", false, false, false, false,
                "BigBench engine validation: Power test"),

        ENGINE_VALIDATION_RESULT_VALIDATION("validateQuery", "engine_validation_power_test", false, false, true, false,
                "BigBench engine validation: Check all query results"),

        CLEAN_POWER_TEST("cleanQuery", "power_test", false, false, false, false,
                "BigBench clean: Clean power test queries"),

        CLEAN_THROUGHPUT_TEST_1("cleanQuery", "throughput_test_1", false, false, false, false,
                "BigBench clean: Clean first throughput test queries"),

        CLEAN_THROUGHPUT_TEST_2("cleanQuery", "throughput_test_2", false, false, false, false,
                "BigBench clean: Clean second throughput test queries"),

        CLEAN_LOAD_TEST("cleanMetastore", "metastore", false, false, false, false, "BigBench clean: Load test"),

        CLEAN_DATA("cleanData", "data", false, false, false, false, "BigBench clean: Data"),

        DATA_GENERATION("dataGen", "data", false, false, false, true, "BigBench preparation: Data generation"),

        LOAD_TEST("populateMetastore", "metastore", false, false, false, true, "BigBench phase 1: Load test"),

        POWER_TEST("runQuery", "power_test", false, true, false, false, "BigBench phase 2: Power test"),

        THROUGHPUT_TEST(null, "throughput_test", false, false, false, false, "BigBench phase 3: Throughput test"),

        THROUGHPUT_TEST_1("runQuery", "throughput_test_1", true, true, false, false,
                "BigBench phase 3: First throughput test run"),

        THROUGHPUT_TEST_REFRESH("refreshMetastore", "throughput_test_refresh", false, false, false, false,
                "BigBench phase 3: Throughput test data refresh"),

        THROUGHPUT_TEST_2("runQuery", "throughput_test_2", true, true, false, false,
                "BigBench phase 3: Second throughput test run"),

        VALIDATE_POWER_TEST("validateQuery", "power_test", false, false, true, false,
                "BigBench validation: Power test results"),

        VALIDATE_THROUGHPUT_TEST_1("validateQuery", "throughput_test_1", false, false, true, false,
                "BigBench validation: First throughput test results"),

        VALIDATE_THROUGHPUT_TEST_2("validateQuery", "throughput_test_2", false, false, true, false,
                "BigBench validation: Second throughput test results"),

        SHOW_TIMES("showTimes", "show_times", false, false, true, false, "BigBench: show query times"),

        SHOW_ERRORS("showErrors", "show_errors", false, false, true, false, "BigBench: show query errors"),

        SHOW_VALIDATION("showValidation", "show_validation", false, false, true, false,
                "BigBench: show query validation results");

        private String runModule;
        private String namePattern;
        private boolean queryOrderRandom;
        private boolean queryOrderCached;
        private boolean printStdOut;
        private boolean mustSucceed;
        private String consoleMessage;
        private boolean phaseDone;

        private BenchmarkPhase(String runModule, String namePattern, boolean queryOrderRandom, boolean queryOrderCached, boolean printStdOut, boolean mustSucceed, String consoleMessage) {
            this.runModule = runModule;
            this.namePattern = namePattern;
            this.queryOrderRandom = queryOrderRandom;
            this.queryOrderCached = queryOrderCached;
            this.printStdOut = printStdOut;
            this.mustSucceed = mustSucceed;
            this.consoleMessage = consoleMessage;
            phaseDone = false;
        }

        public String getRunModule() {
            return runModule;
        }

        public String getNamePattern() {
            return namePattern;
        }

        public String getConsoleMessage() {
            return consoleMessage;
        }

        public boolean isPhaseDone() {
            return phaseDone;
        }

        public void setPhaseDone(boolean phaseDone) {
            this.phaseDone = phaseDone;
        }

        public String getQueryListProperty(int streamNumber) {
            return namePattern + "_" + streamNumber;
        }

        public boolean isQueryOrderRandom() {
            return queryOrderRandom;
        }

        public boolean isQueryOrderCached() {
            return queryOrderCached;
        }

        public boolean isPrintStdOut() {
            return printStdOut;
        }

        public boolean mustSucceed() {
            return mustSucceed;
        }
    }

    public BigBench() throws SecurityException, IOException {
        parseEnvironment();


        log = Logger.getLogger(getClass().getName());

        log.setUseParentHandlers(false);
        ConsoleHandler ch = new ConsoleHandler();
        ch.setFormatter(new BigBenchFormatter());
        log.addHandler(ch);
        String logFile = logDir + "/BigBenchResult.log";
        FileHandler fh = new FileHandler(logFile, false);
        fh.setFormatter(new SimpleFormatter());
        log.addHandler(fh);

        logTreeRoot = new LogTree("root", null);
        queryListCache = new HashMap();
        executor = Executors.newFixedThreadPool(numberOfParallelStreams);

        logTreeIDForCSVPrint = 1L;

        long seed = 1234567890L;
        rnd = new BigBenchRandom(seed);
    }

    private void parseEnvironment() throws IOException {
        Map<String, String> env = System.getenv();


        version = ((String) env.get("BIG_BENCH_VERSION"));
        homeDir = ((String) env.get("BIG_BENCH_HOME"));
        confDir = ((String) env.get("BIG_BENCH_CONF_DIR"));
        runScript = ((String) env.get("BIG_BENCH_BIN_DIR") + "/bigBench");
        datagenDir = ((String) env.get("BIG_BENCH_DATA_GENERATOR_DIR"));
        logDir = ((String) env.get("BIG_BENCH_LOGS_DIR"));
        dataGenLogFile = ((String) env.get("BIG_BENCH_DATAGEN_STAGE_LOG"));
        loadLogFile = ((String) env.get("BIG_BENCH_LOADING_STAGE_LOG"));
        engine = ((String) env.get("BIG_BENCH_ENGINE"));
        database = ((String) env.get("BIG_BENCH_DATABASE"));
        mapTasks = ((String) env.get("BIG_BENCH_MAP_TASKS"));
        numberOfParallelStreams = Integer.parseInt((String) env.get("BIG_BENCH_NUMBER_OF_PARALLEL_STREAMS"));
        scaleFactor = Long.parseLong((String) env.get("BIG_BENCH_SCALE_FACTOR"));
        stopAfterFailure = ((String) env.get("BIG_BENCH_STOP_AFTER_FAILURE")).equals("1");

        String userProvidedOptions = System.getenv("LIST_OF_USER_OPTIONS").trim();

        if (userProvidedOptions.isEmpty()) {
            userArguments = new ArrayList();
        } else {
            userArguments = new ArrayList(
                    Arrays.asList(userProvidedOptions.replaceAll("\\s+", " ").split(" ")));
        }

        userArguments.add("-U");


        properties = new Properties();
        String propertiesFilename = confDir + "/bigBench.properties";
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(propertiesFilename);
            br = new BufferedReader(fr);
            properties.load(br);
        } finally {
            if (fr != null) {
                fr.close();
            }
            if (br != null) {
                br.close();
            }
        }


        if (env.containsKey("USER_PRETEND_MODE")) {
            properties.setProperty("pretend_mode", (String) env.get("USER_PRETEND_MODE"));
        }

        if (env.containsKey("USER_PRINT_STD_OUT")) {
            properties.setProperty("show_command_stdout", (String) env.get("USER_PRINT_STD_OUT"));
        }

        if (env.containsKey("USER_DRIVER_WORKLOAD")) {
            properties.setProperty("workload", (String) env.get("USER_DRIVER_WORKLOAD"));
        }

        if (env.containsKey("USER_DRIVER_QUERIES_TO_RUN")) {
            properties.setProperty(BenchmarkPhase.POWER_TEST.getQueryListProperty(0),
                    (String) env.get("USER_DRIVER_QUERIES_TO_RUN"));
        }


        pretendMode = properties.getProperty("pretend_mode").equals("1");
        printStdOut = properties.getProperty("show_command_stdout").equals("1");


        benchmarkPhases = new ArrayList();
        for (String benchmarkPhase : Arrays.asList(properties.getProperty("workload").split(","))) {
            benchmarkPhases.add(BenchmarkPhase.valueOf(benchmarkPhase.trim()));
        }


        if (!benchmarkPhases.contains(BenchmarkPhase.BENCHMARK_START)) {
            benchmarkPhases.add(0, BenchmarkPhase.BENCHMARK_START);
        }
        if (!benchmarkPhases.contains(BenchmarkPhase.BENCHMARK_STOP)) {
            benchmarkPhases.add(BenchmarkPhase.BENCHMARK_STOP);
        }
    }

    private void shuffleList(List<?> list, BigBenchRandom rng) {
        int size = list.size();
        for (int i = size; i > 1; i--)
            swapListElements(list, i - 1, rng.nextInt(i));
    }

    private <E> void swapListElements(List<E> list, int i, int j) {
        List<E> l = list;
        l.set(i, l.set(j, l.get(i)));
    }

    public void run() {
        List<String> validationArguments = new ArrayList();
        validationArguments.add("-f");
        validationArguments.add("1");
        validationArguments.add("-e");
        validationArguments.add(engine);
        validationArguments.add("-d");
        validationArguments.add(database);
        validationArguments.add("-m");
        validationArguments.add(mapTasks);
        validationArguments.add("-U");
        try {
            long benchmarkStart = 0L;

            log.info("Benchmark phases: " + benchmarkPhases);

            for (BenchmarkPhase currentPhase : benchmarkPhases) {
                if (currentPhase.isPhaseDone()) {
                    log.info(
                            "The phase " + currentPhase.name() + " was already performed earlier. Skipping this phase");
                } else {
                    try {
                        switch (currentPhase) {

                            case CLEAN_POWER_TEST:
                                generateData(currentPhase, false, validationArguments);
                                break;

                            case CLEAN_THROUGHPUT_TEST_1:
                                generateData(currentPhase, true, validationArguments);
                                break;

                            case CLEAN_LOAD_TEST:
                            case CLEAN_THROUGHPUT_TEST_2:
                                runModule(currentPhase, validationArguments);
                                break;

                            case CLEAN_DATA:
                            case DATA_GENERATION:
                            case ENGINE_VALIDATION_CLEAN_DATA:
                                runQueries(currentPhase, 1, validationArguments);
                                break;
                            case ENGINE_VALIDATION_POWER_TEST:
                                generateData(currentPhase, false, userArguments);
                                break;

                            case ENGINE_VALIDATION_RESULT_VALIDATION:
                                generateData(currentPhase, true, userArguments);
                                break;

                            case BENCHMARK_START:
                                log.info(currentPhase.getConsoleMessage());
                                benchmarkStart = System.currentTimeMillis();
                                break;

                            case BENCHMARK_STOP:
                                if (!BenchmarkPhase.BENCHMARK_START.isPhaseDone()) {
                                    throw new IllegalArgumentException("Error: Cannot stop the benchmark before starting it");
                                }
                                long benchmarkEnd = System.currentTimeMillis();
                                log.info(String.format("%-55s finished. Time: %25s", new Object[]{currentPhase.getConsoleMessage(),
                                        Helper.formatTime(benchmarkEnd - benchmarkStart)}));

                                logTreeRoot.setCheckpoint(new Checkpoint(BenchmarkPhase.BENCHMARK, -1L, -1L, benchmarkStart,
                                        benchmarkEnd, logTreeRoot.isSuccessful()));
                                break;
                            case CLEAN_ALL:
                            case ENGINE_VALIDATION_LOAD_TEST:
                            case LOAD_TEST:
                            case SHOW_VALIDATION:
                            case VALIDATE_POWER_TEST:
                            case VALIDATE_THROUGHPUT_TEST_1:
                            case VALIDATE_THROUGHPUT_TEST_2:
                                runModule(currentPhase, userArguments);
                                break;
                            case ENGINE_VALIDATION_CLEAN_LOAD_TEST:
                            case POWER_TEST:
                            case THROUGHPUT_TEST_1:
                                runQueries(currentPhase, 1, userArguments);
                                break;
                            case ENGINE_VALIDATION_CLEAN_POWER_TEST:
                            case ENGINE_VALIDATION_DATA_GENERATION:
                            case SHOW_TIMES:
                            case THROUGHPUT_TEST:
                            case THROUGHPUT_TEST_2:
                            case THROUGHPUT_TEST_REFRESH:
                                runQueries(currentPhase, numberOfParallelStreams, userArguments);
                                break;
                            case BENCHMARK:
                            case SHOW_ERRORS:
                                throw new IllegalArgumentException(
                                        "The value " + currentPhase.name() + " is only used internally.");
                        }

                        currentPhase.setPhaseDone(true);
                    } catch (IOException e) {
                        log.info(
                                "==============\nBenchmark run terminated\nReason: An error occured while running a command in phase " +
                                        currentPhase + "\n==============");
                        e.printStackTrace();
                        if (stopAfterFailure) break;
                    }
                    if (currentPhase.mustSucceed()) {
                        break;
                    }
                }
            }


            if ((BenchmarkPhase.THROUGHPUT_TEST_1.isPhaseDone()) && (BenchmarkPhase.THROUGHPUT_TEST_2.isPhaseDone())) {
                BenchmarkPhase.THROUGHPUT_TEST.setPhaseDone(true);


                Map<Object, LogTree> children = logTreeRoot.getChildren();


                Checkpoint startCheckpoint = ((LogTree) children.get(BenchmarkPhase.THROUGHPUT_TEST_1)).getCheckpoint();

                long throughputStart = startCheckpoint.getStartTimestamp();


                Checkpoint endCheckpoint = ((LogTree) children.get(BenchmarkPhase.THROUGHPUT_TEST_2)).getCheckpoint();

                long throughputEnd = endCheckpoint.getEndTimestamp();

                LogTree throughputLogTree = new LogTree(BenchmarkPhase.THROUGHPUT_TEST, logTreeRoot);
                children.put(BenchmarkPhase.THROUGHPUT_TEST, throughputLogTree);
                throughputLogTree.setCheckpoint(new Checkpoint(BenchmarkPhase.THROUGHPUT_TEST, -1L, -1L, throughputStart,
                        throughputEnd, (startCheckpoint.isSuccessful()) && (endCheckpoint.isSuccessful())));
                if ((!((LogTree) children.get(BenchmarkPhase.THROUGHPUT_TEST_1)).isSuccessful()) ||
                        (!((LogTree) children.get(BenchmarkPhase.THROUGHPUT_TEST_2)).isSuccessful())) {
                    throughputLogTree.setFailed();
                }
            }


            computeResult();
            writeLogTreeToCSV();
        } catch (InterruptedException e) {
            log.info(
                    "==============\nBenchmark run terminated\nReason: A waiting thread was interrupted\n==============");
            e.printStackTrace();

            log.info("DEBUG: Shutting down the thread pool now...");
            executor.shutdownNow();
            try {
                log.info("DEBUG: Waiting 5s for threads to stop");
                if (!executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                    log.info("DEBUG: Threadpool could not be killed. Exiting with 1");
                    System.exit(1);
                }
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        } finally {
            log.info("DEBUG: Shutting down the thread pool now...");
            executor.shutdownNow();
            try {
                log.info("DEBUG: Waiting 5s for threads to stop");
                if (!executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                    log.info("DEBUG: Threadpool could not be killed. Exiting with 1");
                    System.exit(1);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean runCmd(String workingDirectory, boolean printStdOut, String... command) {
        log.info("DEBUG: Running command: " + new ArrayList(Arrays.asList(command)) + " in directory " +
                workingDirectory);

        if (pretendMode) {
            try {
                Thread.sleep(new Random().nextInt(1000));
            } catch (InterruptedException e) {
                log.info("Pretend wait thread was interrupted");
                return false;
            }

            return true;
        }
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(new File(workingDirectory));
        Process p = null;
        try {
            p = pb.start();

            Thread stdOutReader = new StreamHandler(p.getInputStream(), System.out, printStdOut);
            Thread stdErrReader = new StreamHandler(p.getErrorStream(), System.err, printStdOut);

            stdOutReader.start();
            stdErrReader.start();

            stdOutReader.join();
            stdErrReader.join();

            int returnValue = p.waitFor();
            log.info("Command: " + Arrays.toString(command) + "got return value: " + returnValue);
            if (returnValue != 0) {
                log.info("Command: " + Arrays.toString(command) + "\ngot non-zero return value: " + returnValue);
            }
            return returnValue == 0;
        } catch (InterruptedException e) {
            log.info("stdout read thread was interrupted");
            return false;
        } catch (IOException e) {
            log.info("I/O exception when running the command: " + command);
            return false;
        } finally {
            if (p != null) {
                p.destroy();
            }
        }
    }

    private List<Integer> getQueryList(BenchmarkPhase benchmarkPhase, int streamNumber) {
        String SHUFFLED_NAME_PATTERN = "shuffledQueryList";
        BenchmarkPhase queryOrderBasicPhase = BenchmarkPhase.POWER_TEST;

        String propertyKey = benchmarkPhase.getQueryListProperty(streamNumber);
        boolean queryOrderCached = benchmarkPhase.isQueryOrderCached();

        if ((queryOrderCached) && (queryListCache.containsKey(propertyKey))) {
            return new ArrayList((Collection) queryListCache.get(propertyKey));
        }

        List<Integer> queryList;

        if (properties.containsKey(propertyKey)) {
            queryList = new ArrayList();
            for (String query : properties.getProperty(propertyKey).split(",")) {
                String[] queryRange = query.trim().split("-");

                switch (queryRange.length) {
                    case 1:
                        queryList.add(Integer.valueOf(Integer.parseInt(queryRange[0].trim())));
                        break;

                    case 2:
                        int startQuery = Integer.parseInt(queryRange[0]);
                        int endQuery = Integer.parseInt(queryRange[1]);
                        if (startQuery > endQuery) {
                            for (int i = startQuery; i >= endQuery; i--) {
                                queryList.add(Integer.valueOf(i));
                            }
                        } else {
                            for (int i = startQuery; i <= endQuery; i++) {
                                queryList.add(Integer.valueOf(i));
                            }
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Query numbers must be in the form X or X-Y, comma separated.");
                }

            }
        } else if (benchmarkPhase.isQueryOrderRandom()) {
            if (!queryListCache.containsKey("shuffledQueryList")) {


                String basicPhaseNamePattern = queryOrderBasicPhase.getQueryListProperty(0);
                if (properties.containsKey(basicPhaseNamePattern)) {

                    queryListCache.put("shuffledQueryList", getQueryList(queryOrderBasicPhase, 0));
                } else {
                    throw new IllegalArgumentException("Property " + basicPhaseNamePattern +
                            " is not deinfod, but is the basis for shuffling the query list.");
                }
            }
            queryList = (List) queryListCache.get("shuffledQueryList");
            shuffleList(queryList, rnd);
        } else {
            queryList = getQueryList(queryOrderBasicPhase, 0);
        }


        if (queryOrderCached) {
            queryListCache.put(propertyKey, new ArrayList(queryList));
        }
        return new ArrayList(queryList);
    }

    private void runQueries(BenchmarkPhase benchmarkPhase, int numberOfStreamsToRun, List<String> arguments)
            throws IOException, InterruptedException {
        String logLineStart = "%-55s Running queries on %10s stream(s)";
        String logLineEnd = "%-55s finished. Time: %25s";
        String logLineError = "%-55s failed. Time: %25s";

        log.info(String.format("%-55s Running queries on %10s stream(s)", new Object[]{benchmarkPhase.getConsoleMessage(), Integer.valueOf(numberOfStreamsToRun)}));

        LogTree currentPhaseLogTree = new LogTree(benchmarkPhase, logTreeRoot);

        long startTimestamp = System.currentTimeMillis();

        log.info("DEBUG: runQueries running " + numberOfStreamsToRun + " streams");

        BlockingQueue<ThreadResult<LogTree>> threadResults = new LinkedBlockingQueue();

        log.info("DEBUG: runQueries starting threads...");

        for (int i = 0; i < numberOfStreamsToRun; i++) {
            LogTree queryThreadLogTree = new LogTree(Integer.valueOf(i), currentPhaseLogTree);
            executor.submit(new QueryThread(benchmarkPhase, i, arguments, getQueryList(benchmarkPhase, i),
                    threadResults, queryThreadLogTree));
        }

        log.info("DEBUG: runQueries getting results...");
        for (int i = 0; i < numberOfStreamsToRun; i++) {
            ThreadResult<LogTree> result = (ThreadResult) threadResults.take();

            if (result.getThreadException() == null) {
                log.info("DEBUG: runQueries received good result");
            } else {
                log.info("DEBUG: runQueries received bad result");

                long endTimestamp = System.currentTimeMillis();

                log.info(String.format("%-55s failed. Time: %25s", new Object[]{benchmarkPhase.getConsoleMessage(),
                        Helper.formatTime(endTimestamp - startTimestamp)}));

                if ((stopAfterFailure) || (benchmarkPhase.mustSucceed())) {
                    currentPhaseLogTree.setCheckpoint(new Checkpoint(benchmarkPhase, -1L, -1L, startTimestamp,
                            endTimestamp, currentPhaseLogTree.isSuccessful()));
                    throw result.getThreadException();
                }
            }
        }

        long endTimestamp = System.currentTimeMillis();

        log.info(String.format("%-55s finished. Time: %25s", new Object[]{benchmarkPhase.getConsoleMessage(),
                Helper.formatTime(endTimestamp - startTimestamp)}));

        currentPhaseLogTree.setCheckpoint(new Checkpoint(benchmarkPhase, -1L, -1L, startTimestamp, endTimestamp,
                currentPhaseLogTree.isSuccessful()));
    }

    private void runModule(BenchmarkPhase benchmarkPhase, List<String> arguments) throws IOException {
        String logLineStart = "%-55s (this might take a while...)";
        String logLineEnd = "%-55s finished. Time: %25s";
        String logLineError = "%-55s failed. Time: %25s";

        log.info(String.format("%-55s (this might take a while...)", new Object[]{benchmarkPhase.getConsoleMessage()}));

        LogTree currentPhaseLogTree = new LogTree(benchmarkPhase, logTreeRoot);

        List<String> cmdLine = new ArrayList();

        cmdLine.add("bash");
        cmdLine.add(runScript);
        cmdLine.add(benchmarkPhase.getRunModule());
        cmdLine.addAll(arguments);

        long startTimestamp = System.currentTimeMillis();

        boolean successful = runCmd(homeDir, benchmarkPhase.isPrintStdOut(), (String[]) cmdLine.toArray(new String[0]));

        long endTimestamp = System.currentTimeMillis();

        currentPhaseLogTree
                .setCheckpoint(new Checkpoint(benchmarkPhase, -1L, -1L, startTimestamp, endTimestamp, successful));

        if (successful) {
            log.info(String.format("%-55s finished. Time: %25s", new Object[]{benchmarkPhase.getConsoleMessage(),
                    Helper.formatTime(endTimestamp - startTimestamp)}));
        } else {
            log.info(String.format("%-55s failed. Time: %25s", new Object[]{benchmarkPhase.getConsoleMessage(),
                    Helper.formatTime(endTimestamp - startTimestamp)}));

            propagateError(currentPhaseLogTree);

            if ((stopAfterFailure) || (benchmarkPhase.mustSucceed())) {
                log.info("DEBUG: " + Thread.currentThread().getName() + ", " +
                        " caused run termination. Throwing IOException now...");

                throw new IOException("Error while running module " + benchmarkPhase.getRunModule() +
                        ". More information in corresponding logfile in " + logDir);
            }
        }
    }

    private void generateData(BenchmarkPhase benchmarkPhase, boolean checkLicense, List<String> arguments)
            throws IOException, InterruptedException {
        if ((checkLicense) && (!pretendMode)) {
            acceptPdgfLicense();
        }

        String logLineStart = "%-55s (this might take a while...)";
        String logLineEnd = "%-55s finished. Time: %25s";

        log.info(String.format("%-55s (this might take a while...)", new Object[]{benchmarkPhase.getConsoleMessage()}));

        LogTree currentPhaseLogTree = new LogTree(benchmarkPhase, logTreeRoot);

        List<String> cmdLine = new ArrayList();

        cmdLine.add("bash");
        cmdLine.add(runScript);
        cmdLine.add(benchmarkPhase.getRunModule());
        cmdLine.addAll(arguments);

        long startTimestamp = System.currentTimeMillis();

        boolean successful = runCmd(homeDir, benchmarkPhase.isPrintStdOut(), (String[]) cmdLine.toArray(new String[0]));

        long endTimestamp = System.currentTimeMillis();

        log.info(String.format("%-55s finished. Time: %25s", new Object[]{benchmarkPhase.getConsoleMessage(),
                Helper.formatTime(endTimestamp - startTimestamp)}));

        currentPhaseLogTree
                .setCheckpoint(new Checkpoint(benchmarkPhase, -1L, -1L, startTimestamp, endTimestamp, successful));

        if (!successful) {
            propagateError(currentPhaseLogTree);
            throw new IOException("Error while generating dataset. More information in logfile: " + dataGenLogFile);
        }
    }

    private void acceptPdgfLicense() throws IOException {
        String licenseFilePath = datagenDir + "/LICENSE.txt";
        String propertiesFilePath = datagenDir + "/Constants.properties";

        File licenseFile = new File(licenseFilePath);
        File propertiesFile = new File(propertiesFilePath);
        String licenseAcceptedKey = "IS_EULA_ACCEPTED";


        String line = null;
        BufferedReader propertiesReader = new BufferedReader(new FileReader(propertiesFile));

        Properties props = new Properties();
        props.load(propertiesReader);

        propertiesReader.close();


        if ((!props.containsKey(licenseAcceptedKey)) || (props.get(licenseAcceptedKey).equals("false"))) {
            System.out.println(
                    "By using this software you must first agree to our terms of use. Press [ENTER] to show them.");


            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String enter = br.readLine();


            BufferedReader licenseReader = new BufferedReader(new FileReader(licenseFile));

            while ((line = licenseReader.readLine()) != null) {
                System.out.println(line);
            }
            licenseReader.close();

            System.out.println("If you have read and agree to these terms of use, please type (uppercase!): YES and press [ENTER]");
            props.setProperty(licenseAcceptedKey, String.valueOf(br.readLine().trim().equals("YES")));

            br.close();


            BufferedWriter propertiesWriter = new BufferedWriter(new FileWriter(propertiesFile));

            props.store(propertiesWriter, "");

            propertiesWriter.close();

            if (props.get(licenseAcceptedKey).equals("false")) {
                throw new IllegalArgumentException("PDGF license was not accepted");
            }
        }
    }

    private void computeResult() {
        double EPSILON = 1.0E-7D;

        double loadTimeInSeconds = 0.0D;
        double loadTestResult = 0.0D;
        double powerTestResult = 1.0D;
        double throughputFirstRunTimeInSeconds = 0.0D;
        double throughputFirstRunTestResult = 0.0D;

        double numberOfStreams = numberOfParallelStreams;
        double totalNumberOfPowerQueries = 0.0D;
        double totalNumberOfFirstThroughputQueries = 0.0D;

        log.info("\n************\nTPCx-BB\nResult\nv" + version + "\n************");


        if (BenchmarkPhase.LOAD_TEST.isPhaseDone()) {
            loadTimeInSeconds = getDuration(BenchmarkPhase.LOAD_TEST) / 1000.0D;
            loadTestResult = 0.1D * loadTimeInSeconds;

            log.info("T_LOAD = " + loadTimeInSeconds);
            log.info("T_LD = 0.1 * T_LOAD: " + loadTestResult);
        }


        if (BenchmarkPhase.POWER_TEST.isPhaseDone()) {
            LogTree powerTestLogTree = (LogTree) ((LogTree) logTreeRoot.getChildren().get(BenchmarkPhase.POWER_TEST)).getChildren().get(Integer.valueOf(0));
            for (Map.Entry<Object, LogTree> powerTestChild : powerTestLogTree.getChildren().entrySet()) {
                double queryDurationInSeconds = ((LogTree) powerTestChild.getValue()).getCheckpoint().getDuration() /
                        1000.0D;
                powerTestResult *= queryDurationInSeconds;
            }

            totalNumberOfPowerQueries = powerTestLogTree.getChildren().size();

            powerTestResult = Math.pow(powerTestResult, 1.0D / totalNumberOfPowerQueries);
            powerTestResult *= totalNumberOfPowerQueries;

            log.info("T_PT = " + powerTestResult);
        }


        if (BenchmarkPhase.THROUGHPUT_TEST_1.isPhaseDone()) {
            throughputFirstRunTimeInSeconds = getDuration(BenchmarkPhase.THROUGHPUT_TEST_1) / 1000.0D;
            throughputFirstRunTestResult = throughputFirstRunTimeInSeconds / numberOfStreams;

            log.info("T_T_PUT = " + throughputFirstRunTimeInSeconds);
            log.info("T_TT = " + throughputFirstRunTestResult);
        }
        boolean queriesOK;
        if ((BenchmarkPhase.LOAD_TEST.isPhaseDone()) && (BenchmarkPhase.POWER_TEST.isPhaseDone()) &&
                (BenchmarkPhase.THROUGHPUT_TEST_1.isPhaseDone())) {
            boolean isResultValid = true;

            log.info("=== Checking validity of the final result ===");
            log.info("OK: All required BigBench phases were performed.");

            double numerator = scaleFactor * 60.0D * totalNumberOfPowerQueries;
            double denominator = loadTestResult + Math.pow(powerTestResult * throughputFirstRunTestResult, 0.5D);
            if ((denominator + 1.0E-7D >= 0.0D) && (denominator - 1.0E-7D <= 0.0D)) {
                throw new IllegalArgumentException("Division by zero!");
            }

            if (BenchmarkPhase.POWER_TEST.isPhaseDone()) {
                List<Integer> powerTestQueryList =
                        (List) queryListCache.get(BenchmarkPhase.POWER_TEST.getQueryListProperty(0));
                if ((powerTestQueryList.containsAll(BIG_BENCH_DEFAULT_QUERIES)) &&
                        (BIG_BENCH_DEFAULT_QUERIES.containsAll(powerTestQueryList))) {
                    log.info(
                            "OK: All " + BIG_BENCH_DEFAULT_QUERIES.size() + " queries were running in the power test.");
                } else {
                    log.info("NOK: Not all " + BIG_BENCH_DEFAULT_QUERIES.size() +
                            " queries were running in the power test.");
                    isResultValid = false;
                }
            }

            if (BenchmarkPhase.THROUGHPUT_TEST_1.isPhaseDone()) {
                queriesOK = true;

                for (int i = 0; i < numberOfParallelStreams; i++) {
                    List<Integer> currentQueryList =
                            (List) queryListCache.get(BenchmarkPhase.THROUGHPUT_TEST_1.getQueryListProperty(i));
                    if ((!currentQueryList.containsAll(BIG_BENCH_DEFAULT_QUERIES)) ||
                            (!BIG_BENCH_DEFAULT_QUERIES.containsAll(currentQueryList))) {
                        log.info("NOK: Not all " + BIG_BENCH_DEFAULT_QUERIES.size() +
                                " queries were running in the first throughput test in stream " + i + ".");
                        queriesOK = false;
                    }
                }

                if (queriesOK) {
                    log.info("OK: All " + BIG_BENCH_DEFAULT_QUERIES.size() +
                            " queries were running in the first throughput test.");
                } else {
                    log.info("NOK: Not all " + BIG_BENCH_DEFAULT_QUERIES.size() +
                            " queries were running in the first throughput test.");
                    isResultValid = false;
                }
            }

            if (!pretendMode) {
                log.info("OK: Pretend mode was inactive. All commands were executed.");
            } else {
                log.info("NOK: Pretend mode was active. No commands were executed.");
                isResultValid = false;
            }

            log.info("=== Final result ===");
            if (isResultValid) {
                log.info("VALID BBQpm@" + scaleFactor + " = " + numerator / denominator);
            } else {
                log.info("INVALID BBQpm@" + scaleFactor + " = " + numerator / denominator);
            }
        } else {
            log.info("NOK: Some required BigBench phases were skipped. No final result available.");
        }

        log.info("DEBUG: " + logTreeRoot.toString());

        String message = "DEBUG: queryListCache entry: %-25squeries: %-100s";
        for (Map.Entry<String, List<Integer>> entry : queryListCache.entrySet()) {
            StringBuilder queries = new StringBuilder();
            for (Iterator queriesOKIt = ((List) entry.getValue()).iterator(); queriesOKIt.hasNext(); ) {
                int query = ((Integer) queriesOKIt.next()).intValue();
                queries.append(query);
                queries.append(",");
            }
            log.info(String.format("DEBUG: queryListCache entry: %-25squeries: %-100s", new Object[]{entry.getKey(), queries.toString()}));
        }
    }

    private void computeResultOrig() {
        double EPSILON = 1.0E-7D;

        double loadTimeInSeconds = 0.0D;
        double powerTimeInSeconds = 0.0D;
        double throughputFirstRunTimeInSeconds = 0.0D;
        double throughputRefreshTimeInSeconds = 0.0D;
        double throughputSecondRunTimeInSeconds = 0.0D;
        double throughputTimeInSeconds = 0.0D;

        double numberOfStreams = numberOfParallelStreams;
        double totalNumberOfPowerQueries = 0.0D;
        double totalNumberOfFirstThroughputQueries = 0.0D;
        double totalNumberOfSecondThroughputQueries = 0.0D;
        double secondsPerHour = 3600.0D;

        log.info(
                "\n ___ _        ___       _\n| _ |_)__ _  |   \\ __ _| |_ __ _\n| _ \\ / _` | | |) / _` |  _/ _` |\n|___/_\\__, | |___/\\__,_|\\__\\__,_|     _\n| _ ) |___/ _  __| |_  _ __  __ _ _ _| |__\n| _ \\/ -_) ' \\/ _| ' \\| '  \\/ _` | '_| / /\n|___/\\___|_||_\\__|_||_|_|_|_\\__,_|_| |_\\_\\\n| _ \\___ ____  _| | |_\n|   / -_|_-< || | |  _|\n|_|_\\___/__/\\_,_|_|\\__|  v" +
                        version + "\n");


        double denominator = 0.0D;

        boolean isAnyBenchmarkedPhasePerformed = false;


        if (BenchmarkPhase.LOAD_TEST.isPhaseDone()) {
            loadTimeInSeconds = getDuration(BenchmarkPhase.LOAD_TEST) / 1000.0D;

            log.info("Load test duration: " + loadTimeInSeconds + " seconds");
            log.info("Load test result: " + secondsPerHour / loadTimeInSeconds);


            isAnyBenchmarkedPhasePerformed = true;
            denominator += numberOfStreams * loadTimeInSeconds;
        }


        if (BenchmarkPhase.POWER_TEST.isPhaseDone()) {
            powerTimeInSeconds = getDuration(BenchmarkPhase.POWER_TEST) / 1000.0D;

            totalNumberOfPowerQueries = ((List) queryListCache.get(BenchmarkPhase.POWER_TEST.getQueryListProperty(0))).size();

            log.info("Power test duration: " + powerTimeInSeconds + " seconds");
            log.info("Power test result: " + totalNumberOfPowerQueries * secondsPerHour / powerTimeInSeconds);


            isAnyBenchmarkedPhasePerformed = true;
            denominator += numberOfStreams * powerTimeInSeconds;
        }


        if (BenchmarkPhase.THROUGHPUT_TEST_1.isPhaseDone()) {
            throughputFirstRunTimeInSeconds = getDuration(BenchmarkPhase.THROUGHPUT_TEST_1) / 1000.0D;

            totalNumberOfFirstThroughputQueries =
                    ((List) queryListCache.get(BenchmarkPhase.THROUGHPUT_TEST_1.getQueryListProperty(0))).size();

            log.info("Throughput test first query run (" + numberOfParallelStreams + " streams) duration: " +
                    throughputFirstRunTimeInSeconds + " seconds");
            log.info("Throughput test first query run (" + numberOfParallelStreams + " streams) result: " +
                    totalNumberOfFirstThroughputQueries * numberOfStreams * secondsPerHour /
                            throughputFirstRunTimeInSeconds);


            isAnyBenchmarkedPhasePerformed = true;
            denominator += throughputFirstRunTimeInSeconds;
        }


        if (BenchmarkPhase.THROUGHPUT_TEST_REFRESH.isPhaseDone()) {
            throughputRefreshTimeInSeconds = getDuration(BenchmarkPhase.THROUGHPUT_TEST_REFRESH) / 1000.0D;

            log.info("Throughput test refresh duration: " + throughputRefreshTimeInSeconds + " seconds");
            log.info("Throughput test refresh result: " + secondsPerHour / throughputRefreshTimeInSeconds);


            isAnyBenchmarkedPhasePerformed = true;
            denominator += numberOfStreams * throughputRefreshTimeInSeconds;
        }


        if (BenchmarkPhase.THROUGHPUT_TEST_2.isPhaseDone()) {
            throughputSecondRunTimeInSeconds = getDuration(BenchmarkPhase.THROUGHPUT_TEST_2) / 1000.0D;

            totalNumberOfSecondThroughputQueries =
                    ((List) queryListCache.get(BenchmarkPhase.THROUGHPUT_TEST_2.getQueryListProperty(0))).size();

            log.info("Throughput test second query run (" + numberOfParallelStreams + " streams) duration: " +
                    throughputSecondRunTimeInSeconds + " seconds");
            log.info("Throughput test second query run (" + numberOfParallelStreams + " streams) result: " +
                    totalNumberOfSecondThroughputQueries * numberOfStreams * secondsPerHour /
                            throughputSecondRunTimeInSeconds);


            isAnyBenchmarkedPhasePerformed = true;
            denominator += throughputSecondRunTimeInSeconds;
        }


        if ((BenchmarkPhase.THROUGHPUT_TEST_1.isPhaseDone()) && (BenchmarkPhase.THROUGHPUT_TEST_REFRESH.isPhaseDone()) &&
                (BenchmarkPhase.THROUGHPUT_TEST_2.isPhaseDone())) {
            throughputTimeInSeconds = getDuration(BenchmarkPhase.THROUGHPUT_TEST) / 1000.0D;

            log.info("Throughput test duration: " + throughputTimeInSeconds + " seconds");
            log.info("Throughput test result: " +
                    (totalNumberOfFirstThroughputQueries + totalNumberOfSecondThroughputQueries) * numberOfStreams *
                            secondsPerHour / (
                            throughputFirstRunTimeInSeconds +
                                    numberOfParallelStreams * throughputRefreshTimeInSeconds +
                                    throughputSecondRunTimeInSeconds));
        }


        double numerator = (totalNumberOfPowerQueries + totalNumberOfFirstThroughputQueries +
                totalNumberOfSecondThroughputQueries) * numberOfStreams * secondsPerHour;

        boolean isResultValid;
        List<Integer> currentQueryList;
        if (isAnyBenchmarkedPhasePerformed) {
            double result = 0.0D;

            if ((denominator + 1.0E-7D < 0.0D) || (denominator - 1.0E-7D > 0.0D)) {
                result = numerator / denominator;
            } else {
                throw new IllegalArgumentException("Division by zero!");
            }

            isResultValid = true;

            log.info("=== Checking validity of the final BigBench result ===");

            if ((BenchmarkPhase.LOAD_TEST.isPhaseDone()) && (BenchmarkPhase.POWER_TEST.isPhaseDone()) &&
                    (BenchmarkPhase.THROUGHPUT_TEST_1.isPhaseDone())) {
                log.info("OK: All required BigBench phases were performed.");
            } else {
                log.info("NOK: Some required BigBench phases were skipped.");
                isResultValid = false;
            }

            if (BenchmarkPhase.POWER_TEST.isPhaseDone()) {
                List<Integer> powerTestQueryList =
                        (List) queryListCache.get(BenchmarkPhase.POWER_TEST.getQueryListProperty(0));
                if ((powerTestQueryList.containsAll(BIG_BENCH_DEFAULT_QUERIES)) &&
                        (BIG_BENCH_DEFAULT_QUERIES.containsAll(powerTestQueryList))) {
                    log.info(
                            "OK: All " + BIG_BENCH_DEFAULT_QUERIES.size() + " queries were running in the power test.");
                } else {
                    log.info("NOK: Not all " + BIG_BENCH_DEFAULT_QUERIES.size() +
                            " queries were running in the power test.");
                    isResultValid = false;
                }
            }

            if (BenchmarkPhase.THROUGHPUT_TEST_1.isPhaseDone()) {
                boolean queriesOK = true;

                for (int i = 0; i < numberOfParallelStreams; i++) {
                    currentQueryList =
                            (List) queryListCache.get(BenchmarkPhase.THROUGHPUT_TEST_1.getQueryListProperty(i));
                    if ((!currentQueryList.containsAll(BIG_BENCH_DEFAULT_QUERIES)) ||
                            (!BIG_BENCH_DEFAULT_QUERIES.containsAll(currentQueryList))) {
                        log.info("NOK: Not all " + BIG_BENCH_DEFAULT_QUERIES.size() +
                                " queries were running in the first throughput test in stream " + i + ".");
                        queriesOK = false;
                    }
                }

                if (queriesOK) {
                    log.info("OK: All " + BIG_BENCH_DEFAULT_QUERIES.size() +
                            " queries were running in the first throughput test.");
                } else {
                    log.info("NOK: Not all " + BIG_BENCH_DEFAULT_QUERIES.size() +
                            " queries were running in the first throughput test.");
                    isResultValid = false;
                }
            }

            if (BenchmarkPhase.THROUGHPUT_TEST_2.isPhaseDone()) {
                boolean queriesOK = true;

                for (int i = 0; i < numberOfParallelStreams; i++) {
                    currentQueryList =
                            (List) queryListCache.get(BenchmarkPhase.THROUGHPUT_TEST_2.getQueryListProperty(i));
                    if ((!currentQueryList.containsAll(BIG_BENCH_DEFAULT_QUERIES)) ||
                            (!BIG_BENCH_DEFAULT_QUERIES.containsAll(currentQueryList))) {
                        log.info("NOK: Not all " + BIG_BENCH_DEFAULT_QUERIES.size() +
                                " queries were running in the second throughput test in stream " + i + ".");
                        queriesOK = false;
                    }
                }

                if (queriesOK) {
                    log.info("OK: All " + BIG_BENCH_DEFAULT_QUERIES.size() +
                            " queries were running in the second throughput test.");
                } else {
                    log.info("NOK: Not all " + BIG_BENCH_DEFAULT_QUERIES.size() +
                            " queries were running in the second throughput test.");
                    isResultValid = false;
                }
            }

            if (!pretendMode) {
                log.info("OK: Pretend mode was inactive. All commands were executed.");
            } else {
                log.info("NOK: Pretend mode was active. No commands were executed.");
                isResultValid = false;
            }

            log.info("=== BigBench final result ===");
            if (isResultValid) {
                log.info("BigBench result is VALID: " + result);
            } else {
                log.info("BigBench result is INVALID: " + result);
            }
        } else {
            log.info("None of the required BigBench phases were performed.");
            log.info("No BigBench result available.");
        }

        log.info("DEBUG: " + logTreeRoot.toString());

        String message = "DEBUG: queryListCache entry: %-25squeries: %-100s";
        for (Map.Entry<String, List<Integer>> entry : queryListCache.entrySet()) {
            StringBuilder queries = new StringBuilder();
            for (Iterator currentQueryListIt = ((List) entry.getValue()).iterator(); currentQueryListIt.hasNext(); ) {
                int query = ((Integer) currentQueryListIt.next()).intValue();
                queries.append(query);
                queries.append(",");
            }
            log.info(String.format("DEBUG: queryListCache entry: %-25squeries: %-100s", new Object[]{entry.getKey(), queries.toString()}));
        }
    }

    private double getDuration(BenchmarkPhase benchmarkPhase) {
        if (!logTreeRoot.getChildren().containsKey(benchmarkPhase)) {
            throw new NoSuchElementException(
                    "Benchmark phase " + benchmarkPhase.name() + " is not present in the map!");
        }

        return ((LogTree) logTreeRoot.getChildren().get(benchmarkPhase)).getCheckpoint().getDuration();
    }

    private void writeLogTreeToCSV() {
        String filename = logDir + "/BigBenchTimes.csv";

        File file = new File(filename);
        try {
            BufferedWriter output = new BufferedWriter(new FileWriter(file));
            output.write("ID;benchmarkPhase;streamNumber;queryNumber;epochStartTimestamp;epochEndTimestamp;humanReadableStartTime;humanReadableEndTime;durationInMilliseconds;durationInSeconds;humanReadableDuration;queryType;successful;\n");
            writeLogTreeToCSV(logTreeRoot, output);

            output.close();
        } catch (IOException e) {
            log.info("An error occurred when attempting to write the log to csv on disk.");
            e.printStackTrace();
        }
    }

    private void writeLogTreeToCSV(LogTree logTree, Writer output)
            throws IOException {
        Checkpoint currentCheckpoint = logTree.getCheckpoint();
        if (currentCheckpoint != null) {
            output.write(logTreeIDForCSVPrint++ + ";" + currentCheckpoint.toCSV() + "\n");
        }

        for (Map.Entry<Object, LogTree> entry : logTree.getChildren().entrySet()) {
            writeLogTreeToCSV((LogTree) entry.getValue(), output);
        }
    }

    private void propagateError(LogTree startLogTree) {
        startLogTree.setFailed();
        LogTree parent = startLogTree.getParent();
        while (parent != null) {
            parent.setFailed();
            parent = parent.getParent();
        }
    }

    private static class Helper {
        private static final long TIMES_1000 = 1000L;
        private static final long TIMES_3600 = 3600L;
        private static final long TIMES_60 = 60L;
        private static final long BYTES_Kibi_MULTIPLIER = 1024L;
        private static final long BYTES_Mibi_MULTIPLIER = 1048576L;
        private static final long BYTES_Gibi_MULTIPLIER = 1073741824L;
        private static final double BYTES_Kibi_MULTIPLIER_d = 1024.0D;
        private static final double BYTES_Mibi_MULTIPLIER_d = 1048576.0D;
        private static final double BYTES_Gibi_MULTIPLIER_d = 1.073741824E9D;

        private Helper() {
        }

        public static String formatTime(long s) {
            return formatTime(s, TimeUnit.MILLISECONDS);
        }

        public static String formatTime(long s, TimeUnit precision) {
            long sec = s / 1000L;
            switch (precision) {
                case SECONDS:
                    return String.format("%ddays", new Object[]{Long.valueOf(sec / 3600L / 24L)});
                case NANOSECONDS:
                    return String.format("%dh:%02dm:%02ds:%03dms", new Object[]{Long.valueOf(sec / 3600L), Long.valueOf(sec % 3600L / 60L),
                            Long.valueOf(sec % 60L), Long.valueOf(s % 1000L)});
                case MINUTES:
                    return String.format("%dh:%02dm", new Object[]{Long.valueOf(sec / 3600L), Long.valueOf(sec % 3600L / 60L)});
                case MILLISECONDS:
                    return String.format("%dh:%02dm:%02ds", new Object[]{Long.valueOf(sec / 3600L), Long.valueOf(sec % 3600L / 60L),
                            Long.valueOf(sec % 60L)});
            }
            return String.format("%dh:%02dm:%02ds:%03dms", new Object[]{Long.valueOf(sec / 3600L), Long.valueOf(sec % 3600L / 60L),
                    Long.valueOf(sec % 60L), Long.valueOf(s % 1000L)});
        }
    }

    private class ThreadResult<T> {
        private IOException threadException;
        private T threadResult;

        public ThreadResult(T threadResult, IOException threadException) {
            this.threadResult = threadResult;
            this.threadException = threadException;
        }

        public IOException getThreadException() {
            return threadException;
        }

        public void setThreadException(IOException threadException) {
            this.threadException = threadException;
        }

        public T getThreadResult() {
            return threadResult;
        }

        public void setThreadResult(T threadResult) {
            this.threadResult = threadResult;
        }
    }


    private class QueryThread
            implements Callable<Void> {
        private List<Integer> queryList;

        BenchmarkPhase benchmarkPhase;

        private int streamNumber;

        private List<String> arguments;

        private BlockingQueue<ThreadResult<LogTree>> results;

        private LogTree logTree;

        public QueryThread(BenchmarkPhase benchmarkPhase, int streamNumber, List<String> arguments, List<Integer> queryList, BlockingQueue<ThreadResult<LogTree>> results, LogTree logTree) {
            this.benchmarkPhase = benchmarkPhase;
            this.streamNumber = streamNumber;
            this.arguments = arguments;
            this.queryList = queryList;
            this.results = results;
            this.logTree = logTree;
        }


        public Void call() {
            log.info("DEBUG: " + Thread.currentThread().getName() + ", " + queryList);


            int loopCounter = 0;

            String logLineStart = "%-55s stream %10s, query %2s (%2s/%2s) log file: %s";
            String logLineEnd = "%-55s stream %10s, query %2s (%2s/%2s) finished. Time: %25s";
            String logLineError = "%-55s stream %10s, query %2s (%2s/%2s) failed. Time: %25s";

            long threadStart = System.currentTimeMillis();

            for (Iterator localIterator = queryList.iterator(); localIterator.hasNext(); ) {
                int i = ((Integer) localIterator.next()).intValue();
                if (Thread.interrupted()) {
                    return null;
                }
                String LOG_FILE_NAME = logDir + "/q" + (i < 10 ? "0" : "") + i + "_" + engine + "_" +
                        benchmarkPhase.getNamePattern() + "_" + streamNumber + ".log";
                loopCounter++;

                log.info(String.format("%-55s stream %10s, query %2s (%2s/%2s) log file: %s", new Object[]{benchmarkPhase.getConsoleMessage(), Integer.valueOf(streamNumber), Integer.valueOf(i), Integer.valueOf(loopCounter),
                        Integer.valueOf(queryList.size()), LOG_FILE_NAME}));

                LogTree queryLogTree = new LogTree(Integer.valueOf(i), logTree);

                long queryStart = System.currentTimeMillis();
                List<String> cmdLine = new ArrayList();

                cmdLine.add("bash");
                cmdLine.add(runScript);
                cmdLine.add(benchmarkPhase.getRunModule());
                cmdLine.add("-q");
                cmdLine.add(String.valueOf(i));
                cmdLine.add("-p");
                cmdLine.add(benchmarkPhase.getNamePattern());
                cmdLine.add("-t");
                cmdLine.add(String.valueOf(streamNumber));
                cmdLine.addAll(arguments);

                boolean successful = BigBench.this.runCmd(homeDir, benchmarkPhase.isPrintStdOut(), (String[]) cmdLine.toArray(new String[0]));

                long queryEnd = System.currentTimeMillis();
                queryLogTree.setCheckpoint(
                        new Checkpoint(benchmarkPhase, streamNumber, i, queryStart, queryEnd, successful));

                if (successful) {
                    log.info(String.format("%-55s stream %10s, query %2s (%2s/%2s) finished. Time: %25s", new Object[]{benchmarkPhase.getConsoleMessage(), Integer.valueOf(streamNumber), Integer.valueOf(i), Integer.valueOf(loopCounter),
                            Integer.valueOf(queryList.size()), Helper.formatTime(queryEnd - queryStart)}));
                } else {
                    log.info(String.format("%-55s stream %10s, query %2s (%2s/%2s) failed. Time: %25s", new Object[]{benchmarkPhase.getConsoleMessage(), Integer.valueOf(streamNumber), Integer.valueOf(i),
                            Integer.valueOf(loopCounter), Integer.valueOf(queryList.size()), Helper.formatTime(queryEnd - queryStart)}));

                    BigBench.this.propagateError(queryLogTree);

                    if ((stopAfterFailure) || (benchmarkPhase.mustSucceed())) {
                        log.info("DEBUG: " + Thread.currentThread().getName() + ", " + queryList +
                                " caused run termination. Returning IOException now...");

                        long threadEnd = System.currentTimeMillis();
                        logTree.setCheckpoint(new Checkpoint(benchmarkPhase, streamNumber, -1L, threadStart, threadEnd,
                                logTree.isSuccessful()));

                        results.add(new ThreadResult(logTree, new IOException(
                                "Error while running query " + i + ". More information in logfile: " + LOG_FILE_NAME)));
                        return null;
                    }
                }
            }

            results.add(new ThreadResult(logTree, null));

            long threadEnd = System.currentTimeMillis();
            logTree.setCheckpoint(
                    new Checkpoint(benchmarkPhase, streamNumber, -1L, threadStart, threadEnd, logTree.isSuccessful()));

            return null;
        }
    }

    private class Checkpoint {
        private BenchmarkPhase benchmarkPhase;
        private long streamNumber;
        private long queryNumber;
        private long startTimestamp;
        private long endTimestamp;
        private boolean successful;

        public Checkpoint(BenchmarkPhase benchmarkPhase, long streamNumber, long queryNumber, long startTimestamp, long endTimestamp, boolean successful) {
            this.benchmarkPhase = benchmarkPhase;
            this.streamNumber = streamNumber;
            this.queryNumber = queryNumber;
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
            this.successful = successful;
        }

        public BenchmarkPhase getBenchmarkPhase() {
            return benchmarkPhase;
        }

        public long getStreamNumber() {
            return streamNumber;
        }

        public long getQueryNumber() {
            return queryNumber;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public long getEndTimestamp() {
            return endTimestamp;
        }

        public long getDuration() {
            return endTimestamp - startTimestamp;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public String toString() {
            return toCSV();
        }

        public String toCSV() {
            return benchmarkPhase + ";" + (streamNumber >= 0L ? Long.valueOf(streamNumber) : "") + ";" + (queryNumber > 0L ? Long.valueOf(queryNumber) : "") + ";" + startTimestamp + ";" + endTimestamp + ";" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(startTimestamp)) + ";" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(endTimestamp)) + ";" + getDuration() + ";" + String.format("%.3f", new Object[]{Double.valueOf(getDuration() / 1000.0D)}) + ";" + Helper.formatTime(getDuration()) + ";" + (queryNumber > 0L ? BIG_BENCH_DEFAULT_QUERIES_TYPE[((int) queryNumber - 1)] : "") + ";" + successful + ";";
        }
    }

    private class BigBenchFormatter extends SimpleFormatter {
        private BigBenchFormatter() {
        }

        public String format(LogRecord record) {
            return record.getMessage() + "\r\n";
        }
    }

    private class StreamHandler extends Thread {
        private InputStream is;
        private PrintStream ps;
        private boolean phasePrintStdOut;

        public StreamHandler(InputStream is, PrintStream ps, boolean phasePrintStdOut) {
            this.is = is;
            this.ps = ps;
            this.phasePrintStdOut = phasePrintStdOut;
            setDaemon(true);
        }

        public void run() {
            boolean globalPrintStdOut = printStdOut;
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String line;
                while ((line = br.readLine()) != null) {
                    if ((phasePrintStdOut) || (globalPrintStdOut)) {
                        ps.println(line);
                    }
                }
                is.close();
                br.close();
            } catch (IOException e) {
                System.err.println("Error while closing streams");
                e.printStackTrace();
            }
        }
    }

    private class LogTree {
        Object id;
        LogTree parent;
        boolean successful;
        Checkpoint checkpoint;
        Map<Object, LogTree> children;

        public LogTree(Object id, LogTree parent) {
            this.id = id;
            this.parent = parent;
            successful = true;
            children = new TreeMap();
            if (parent != null) {
                parent.getChildren().put(id, this);
            }
        }

        public Object getId() {
            return id;
        }

        public LogTree getParent() {
            return parent;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public void setFailed() {
            successful = false;
        }

        public Checkpoint getCheckpoint() {
            return checkpoint;
        }

        public void setCheckpoint(Checkpoint checkpoint) {
            this.checkpoint = checkpoint;
        }

        public Map<Object, LogTree> getChildren() {
            return children;
        }

        public String toString() {
            StringBuilder result = new StringBuilder();

            result.append("local checkpoint " + id + ": " + checkpoint + "\n");

            for (Map.Entry<Object, LogTree> entry : children.entrySet()) {
                result.append("child map: " + entry.getKey() + "\n");
                result.append(entry.getValue() + "\n");
            }

            return result.toString();
        }
    }
}