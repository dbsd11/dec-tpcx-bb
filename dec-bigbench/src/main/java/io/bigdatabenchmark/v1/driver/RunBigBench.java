package io.bigdatabenchmark.v1.driver;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

public class RunBigBench
{
    public static void main(String[] args)
    {
        try
        {
            checkEnvironment();

            BigBench benchmark = new BigBench();
            benchmark.run();
        }
        catch (NumberFormatException e)
        {
            printErrMsg("The provided parameter is not a number");
        }
        catch (SecurityException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static void printErrMsg(String errMsg)
    {
        System.err.println(errMsg + "\n");
        System.exit(1);
    }

    private static void checkEnvironment()
            throws IllegalArgumentException
    {
        String[] envVariables = { "BIG_BENCH_VERSION", "BIG_BENCH_HOME", "BIG_BENCH_BIN_DIR",
                "BIG_BENCH_CONF_DIR", "BIG_BENCH_DATA_GENERATOR_DIR", "BIG_BENCH_LOGS_DIR",
                "BIG_BENCH_DATAGEN_STAGE_LOG", "BIG_BENCH_LOADING_STAGE_LOG", "BIG_BENCH_NUMBER_OF_PARALLEL_STREAMS",
                "LIST_OF_USER_OPTIONS", "BIG_BENCH_STOP_AFTER_FAILURE", "BIG_BENCH_ENGINE", "BIG_BENCH_DATABASE",
                "BIG_BENCH_MAP_TASKS", "BIG_BENCH_SCALE_FACTOR" };

        Map<String, String> env = System.getenv();

        String[] arrayOfString1 = envVariables;int j = envVariables.length;
        for (int i = 0; i < j; i++)
        {
            String envVariable = arrayOfString1[i];
            if (!env.containsKey(envVariable)) {
                throw new IllegalArgumentException("Missing env variable: " + envVariable);
            }
        }
        String logDir = (String)env.get("BIG_BENCH_LOGS_DIR");
        File logDirectory = new File(logDir);
        if ((!logDirectory.isDirectory()) &&
                (!logDirectory.mkdirs())) {
            throw new IllegalArgumentException("The path " + logDir + " could not be created!");
        }
    }
}
