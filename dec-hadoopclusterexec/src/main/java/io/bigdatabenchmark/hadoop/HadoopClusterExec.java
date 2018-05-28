package io.bigdatabenchmark.hadoop;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class HadoopClusterExec extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(HadoopClusterExec.class);

    private static final String EXEC_ARGS = "EXEC_STARTUP_ARGS";

    private static final String EXEC_CWD = "EXEC_CWD";
    private static final String HADOOP_CLUSTER_EXEC = "HadoopClusterExec";
    private static final String JVM_ARGS = "HadoopClusterExec.parentJVMArgs";
    private static final String CHILD_JMX_bytes = "HadoopClusterExec.child.JMX.bytes";
    private static final String TASK_NUMBER = "HadoopClusterExec.taskNumber";
    private static final String TASKS = "HadoopClusterExec.tasks";
    private static final String CHILD_MEM_RATIO = "HadoopClusterExec.CHILD_MEM_RATIO";
    private static final String DEBUG_EXCEPTION_MESSAGES = "HadoopClusterExec.DEBUG_EXCEPTION_MESSAGES";
    private static final String TASK_FAIL_ON_NONZERO_RETURNCODE = "HadoopClusterExec.TASK_FAIL_ON_NONZERO_RETURNCODE";
    private static final String NUM_MAPS = "HadoopClusterExec.numMaps";
    private boolean debugExeceptionMessages;

    public HadoopClusterExec() {
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HadoopClusterExec(), args);
        System.exit(res);
    }


    public int run(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, ParseException, URISyntaxException {
        Options opts = new Options();
        opts.addOption(createOption(
                "exec",
                "program and program arguments to run. Available magic values which can be use as arguments:\n\tHadoopClusterExec.parentJVMArgs\n\tHadoopClusterExec.tasks\n\tHadoopClusterExec.taskNumber\nHadoopClusterExec.tasks will be the same value as specified by the mapTasks option.\nHadoopClusterExec.taskNumber will be in range [1, HadoopClusterExec.tasks]",


                "prog_name arg1 arg2 ...", Integer.MAX_VALUE, true));
        opts.addOption(createOption("mapTasks", "number of tasks to start",
                "number", 1, false));
        opts.addOption(createOption(
                "childMemRatio",
                "amount of memory available to the child process, depending on the Xmx argument of this jvm. childMemMax = childMemRatio[0,1] * -Xmx",
                "number", 1, false));

        opts.addOption(createOption("execCWD",
                "working directory for the command", "dir", 1, false));


        OptionBuilder.withArgName("paths");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription(
                "comma separated files to be copied to the map reduce cluster");
        opts.addOption(
                OptionBuilder.create("files2"));

        OptionBuilder.withArgName("paths");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription(
                "comma separated archives to be unarchived on the compute machines.");
        opts.addOption(

                OptionBuilder.create("archives2"));


        OptionBuilder.withDescription(
                "Will throw an exception instead of distributed executing the specified command. This exception will contain all information which would have been used to start the command.");
        opts.addOption(
                OptionBuilder.create("testDebugMessages"));


        OptionBuilder.withDescription(
                "Task will fail with an exception if subprocess returns with a return value not equal to 0");
        opts.addOption(
                OptionBuilder.create("taskFailOnNonZeroReturnValue"));

        if (args.length == 0) {
            usage(opts);
            return -1;
        }
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (cliParser.hasOption("exec")) {
            String[] execArgs = cliParser.getOptionValues("exec");

            job.getConfiguration().setStrings("EXEC_STARTUP_ARGS", execArgs);
            job.setJobName(Arrays.toString(execArgs));
            System.out.println(getClass().getSimpleName() +
                    " -exec option args: " + Arrays.toString(execArgs));
        } else {
            usage(opts);
            return -1;
        }

        if (cliParser.hasOption("mapTasks")) {
            try {
                int tasks = Integer.parseInt(cliParser
                        .getOptionValue("mapTasks"));
                job.getConfiguration().setInt("HadoopClusterExec.numMaps", tasks);
                job.getConfiguration().setInt("mapreduce.job.maps", tasks);

                System.out.println(getClass().getSimpleName() +
                        " -mapTasks: Setting number of map tasks to: " +
                        tasks);
            } catch (NumberFormatException e) {
                System.out.println("mapTasks " +
                        cliParser.getOptionValue("mapTasks") +
                        " is not a number.");
            }
        }


        if (cliParser.hasOption("execCWD")) {
            String execCWD = cliParser.getOptionValue("execCWD");
            job.getConfiguration().set("EXEC_CWD", execCWD);
            System.out.println(getClass().getSimpleName() +
                    " -execCWD: set working directory for exec to: " +
                    execCWD);
        }
        if (cliParser.hasOption("childMemRatio")) {
            String s = cliParser.getOptionValue("childMemRatio");
            try {
                Double ratio = Double.valueOf(Double.parseDouble(s));
                if ((ratio.doubleValue() < 0.0D) || (ratio.doubleValue() > 1.0D)) {
                    throw new NumberFormatException();
                }
                job.getConfiguration().setDouble("HadoopClusterExec.CHILD_MEM_RATIO", ratio.doubleValue());
                System.out.println(getClass().getSimpleName() +
                        " -childMemRatio: set CHILD_MEM_RATIO to: " + ratio);
            } catch (NumberFormatException e) {
                System.out.println("childMemRatio " +
                        cliParser.getOptionValue("mapTasks") +
                        " is not a number between [0,1].");
            }
        }

        if (cliParser.hasOption("testDebugMessages")) {
            job.getConfiguration().setBoolean("HadoopClusterExec.DEBUG_EXCEPTION_MESSAGES", true);
        }

        if (cliParser.hasOption("taskFailOnNonZeroReturnValue")) {
            job.getConfiguration().setBoolean("HadoopClusterExec.TASK_FAIL_ON_NONZERO_RETURNCODE",
                    true);
        }

        try {
            if (cliParser.hasOption("files2")) {
                URI[] tmp = validateFilesToURIs(
                        cliParser.getOptionValue("files2"), conf);

                for (URI u : tmp) {
                    job.addCacheFile(u);
                }
            }
            if (cliParser.hasOption("archives2")) {
                URI[] tmp = validateFilesToURIs(
                        cliParser.getOptionValue("archives2"), conf);

                for (URI u : tmp) {
                    job.addCacheArchive(u);
                }
            }
        } catch (IOException ioe) {
            System.err.println(StringUtils.stringifyException(ioe));
        }

        System.out.println(getDistributedCacheContentsAsString(job));

        job.setSpeculativeExecution(false);
        job.setJarByClass(HadoopClusterExec.class);
        job.setMapperClass(DistributedExecMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullOutputFormat.class);
        job.setInputFormatClass(RangeInputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static URL[] getLibJars(Configuration conf) throws IOException {
        String jars = conf.get("tmpjars");
        if (jars == null) {
            return null;
        }
        String[] files = jars.split(",");
        URL[] cp = new URL[files.length];
        for (int i = 0; i < cp.length; i++) {
            Path tmp = new Path(files[i]);
            cp[i] = FileSystem.getLocal(conf).pathToFile(tmp).toURI().toURL();
        }
        return cp;
    }

    public static final String getDistributedCacheContentsAsString(Job job) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("\n-------Distributed cache content -----------\n");

        sb.append("Cache Archives:\n");
        URI[] tmpURIs = null;
        try {
            tmpURIs = job.getCacheArchives();
        } catch (IOException localIOException) {
        }
        String uri;
        if (tmpURIs != null) {
            for (URI u : tmpURIs) {
                uri = u.toString();
                sb.append(uri).append("\n");

                sb.append("Deflated path: ")
                        .append(uri.substring(uri.lastIndexOf('/') + 1))
                        .append('/').append("\n");
            }
        }
        sb.append("\nCache Files:\n");
        tmpURIs = null;
        try {
            tmpURIs = job.getCacheFiles();
        } catch (IOException localIOException1) {
        }
        if (tmpURIs != null) {
            for (URI u : tmpURIs) {
                sb.append(u).append("\n");
            }
        }
        sb.append("\nArchive Classpaths:\n");
        Path[] tmpPaths = job.getArchiveClassPaths();
        if (tmpPaths != null) {
            for (Path u : tmpPaths) {
                sb.append(u).append("\n");
            }
        }
        sb.append("\nLocal Cache Archives:\n");
        tmpPaths = null;
        try {
            tmpPaths = job.getLocalCacheArchives();
        } catch (IOException localIOException4) {
        }
        if (tmpPaths != null) {
            for (Path u : tmpPaths) {
                sb.append(u).append("\n");
            }
        }
        sb.append("\nLocal Cache Files:\n");
        tmpPaths = null;
        try {
            tmpPaths = job.getLocalCacheFiles();
        } catch (IOException localIOException5) {
        }
        if (tmpPaths != null) {
            for (Path u : tmpPaths) {
                sb.append(u).append("\n");
            }
        }
        sb.append("-------/Distributed cache content -----------\n");

        return sb.toString();
    }


    public static final String getDistributedCacheContentsAsString(Mapper.Context job) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n-------Distributed cache content -----------\n");

        sb.append("Cache Archives:\n");
        URI[] tmpURIs = null;
        try {
            tmpURIs = job.getCacheArchives();
        } catch (IOException localIOException) {
        }
        String uri;
        if (tmpURIs != null) {
            for (URI u : tmpURIs) {
                uri = u.toString();
                sb.append(uri).append("\n");

                sb.append(" Deflated path: ")
                        .append(uri.substring(uri.lastIndexOf('/') + 1))
                        .append('/').append("\n");
            }
        }
        sb.append("\nCache Files:\n");
        tmpURIs = null;
        try {
            tmpURIs = job.getCacheFiles();
        } catch (IOException localIOException1) {
        }
        if (tmpURIs != null) {
            for (URI u : tmpURIs) {
                sb.append(u).append("\n");
            }
        }
        sb.append("\nArchive Classpaths:\n");
        Path[] tmpPaths = job.getArchiveClassPaths();
        if (tmpPaths != null) {
            for (Path u : tmpPaths) {
                sb.append(u).append("\n");
            }
        }
        sb.append("\nLocal Cache Archives:\n");
        tmpPaths = null;
        try {
            tmpPaths = job.getLocalCacheArchives();
        } catch (IOException localIOException4) {
        }
        if (tmpPaths != null) {
            for (Path u : tmpPaths) {
                sb.append(u).append("\n");
            }
        }
        sb.append("\nLocal Cache Files:\n");
        tmpPaths = null;
        try {
            tmpPaths = job.getLocalCacheFiles();
        } catch (IOException localIOException5) {
        }
        if (tmpPaths != null) {
            for (Path u : tmpPaths) {
                sb.append(u).append("\n");
            }
        }
        sb.append("-------/Distributed cache content -----------\n");

        return sb.toString();
    }

    private void usage(Options opts) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hadoop -jar " + getClass().getSimpleName(),
                opts);
        GenericOptionsParser.printGenericCommandUsage(System.out);
    }


    private Option createOption(String name, String desc, String argName, int max, boolean required) {
        OptionBuilder.withArgName(argName);
        OptionBuilder.hasArgs(max);
        OptionBuilder.withDescription(desc);
        OptionBuilder.isRequired(required);
        return OptionBuilder.create(name);
    }

    private URI[] validateFilesToURIs(String files, Configuration conf) throws IOException {
        if (files == null)
            return null;
        String[] fileArr = files.split(",");
        URI[] finalArr = new URI[fileArr.length];
        for (int i = 0; i < fileArr.length; i++) {
            String tmp = fileArr[i];

            Path path = new Path(tmp);
            URI pathURI = path.toUri();
            FileSystem localFs = FileSystem.getLocal(conf);
            URI finalPath;
            if (pathURI.getScheme() == null) {

                if (!localFs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp +
                            " does not exist.");
                }
                finalPath = path.makeQualified(localFs).toUri();

            } else {

                FileSystem fs = path.getFileSystem(conf);
                if (!fs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp +
                            " does not exist.");
                }
                finalPath = path.makeQualified(fs).toUri();
                try {
                    fs.close();
                } catch (IOException localIOException) {
                }
            }

            finalArr[i] = finalPath;
        }
        return finalArr;
    }

    public static class DistributedStartupInputSplit extends InputSplit implements Writable {
        private long nodeCount;
        private long nodeNumber;

        public DistributedStartupInputSplit() {
        }

        public DistributedStartupInputSplit(long nodeCount, long nodeNumber) {
            this.nodeCount = nodeCount;
            this.nodeNumber = nodeNumber;
        }

        public void readFields(DataInput in) throws IOException {
            nodeCount = WritableUtils.readVLong(in);
            nodeNumber = WritableUtils.readVLong(in);
        }

        public void write(DataOutput out) throws IOException {
            WritableUtils.writeVLong(out, nodeCount);
            WritableUtils.writeVLong(out, nodeNumber);
        }

        public long getLength() throws IOException {
            return 0L;
        }

        public String[] getLocations() throws IOException {
            return new String[0];
        }


        public long getTaskCount() {
            return nodeCount;
        }


        public long getTaskNumber() {
            return nodeNumber;
        }

        public String toString() {
            return
                    "DistributedStartupInputSplit [nodeCount=" + nodeCount + ", nodeNumber=" + nodeNumber + "]";
        }
    }


    public static class RangeInputFormat
            extends InputFormat<DistributedStartupInputSplit, NullWritable> {
        public RangeInputFormat() {
        }


        static class RangeRecordReader
                extends RecordReader<DistributedStartupInputSplit, NullWritable> {
            long nodeCount;

            long nodeNumber;

            private DistributedStartupInputSplit split;

            public RangeRecordReader() {
            }

            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                this.split = ((DistributedStartupInputSplit) split);
                nodeCount = nodeCount;

                nodeNumber = nodeNumber;
            }

            public void close() throws IOException {
            }

            public DistributedStartupInputSplit getCurrentKey() {
                return split;
            }

            public NullWritable getCurrentValue() {
                return NullWritable.get();
            }

            public float getProgress() throws IOException {
                return 0.0F;
            }

            boolean finished = false;

            public boolean nextKeyValue() {
                if (!finished) {
                    finished = true;
                    return true;
                }
                return false;
            }
        }


        public RecordReader<DistributedStartupInputSplit, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException {
            return new RangeRecordReader();
        }


        public List<InputSplit> getSplits(JobContext job) {
            int numSplits = job.getConfiguration().getInt("HadoopClusterExec.numMaps",
                    1);

            HadoopClusterExec.LOG.info("HadoopClusterExec.numMaps=" + numSplits);
            HadoopClusterExec.LOG.info("CUSTOM SPLITS: Starting " + numSplits + " tasks");
            List<InputSplit> splits = new ArrayList();

            for (int split = 0; split < numSplits; split++) {
                splits.add(new DistributedStartupInputSplit(numSplits,
                        split + 1));
            }
            return splits;
        }
    }

    public static class NonZeroReturnCodeException extends Exception {
        public NonZeroReturnCodeException(String msg) {
            super();
        }
    }


    public static class DistributedExecMapper
            extends Mapper<DistributedStartupInputSplit, NullWritable, Text, Text> {
        private String[] args;

        private String cwd;
        private String thisVM_jvm_args;
        private String childVM_JMX_bytes;
        private double childMemRatio;
        private String taskID;
        private boolean debugExceptionMessages;
        private boolean taskFailOnNonZeroReturncode;

        public DistributedExecMapper() {
        }

        protected void setup(Mapper<DistributedStartupInputSplit, NullWritable, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);


            System.out.println("Setup mapper:");

            childMemRatio = context.getConfiguration().getDouble(
                    "HadoopClusterExec.CHILD_MEM_RATIO", 0.6D);
            System.out.println("Job childMemRatio: " + childMemRatio);

            args = context.getConfiguration().getStrings("EXEC_STARTUP_ARGS");
            System.out.println("Job EXEC_STARTUP_ARGS: " +
                    Arrays.toString(args));

            cwd = context.getConfiguration().get("EXEC_CWD", null);
            System.out.println("Job EXEC_CWD: " + cwd);

            debugExceptionMessages = context.getConfiguration().getBoolean(
                    "HadoopClusterExec.DEBUG_EXCEPTION_MESSAGES", false);
            System.out.println("Job HadoopClusterExec.DEBUG_EXCEPTION_MESSAGES: " +
                    debugExceptionMessages);

            taskFailOnNonZeroReturncode = context.getConfiguration()
                    .getBoolean("HadoopClusterExec.TASK_FAIL_ON_NONZERO_RETURNCODE", false);
            System.out.println("Job HadoopClusterExec.TASK_FAIL_ON_NONZERO_RETURNCODE: " +
                    taskFailOnNonZeroReturncode);

            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            List<String> arguments = runtimeMxBean.getInputArguments();
            StringBuilder sb = new StringBuilder();
            for (String arg : arguments) {
                sb.append(" ").append(arg);
            }
            thisVM_jvm_args = sb.toString();

            System.out.println("Argument substitution options:");
            System.out.println("HADOOP_CLUSTER_EXEC_TASKS: HadoopClusterExec.tasks");
            System.out.println("HADOOP_CLUSTER_EXEC_TASK_NUMBER: HadoopClusterExec.taskNumber");

            System.out.println("HADOOP_CLUSTER_EXEC_JVM_ARGS: HadoopClusterExec.parentJVMArgs");

            System.out.println("Local jvm startupArguments(): " + thisVM_jvm_args);
        }

        static enum MyCounters {
            NUM_RECORDS;
        }

        public void map(DistributedStartupInputSplit inputSplit, NullWritable ignored, final Mapper<DistributedStartupInputSplit, NullWritable, Text, Text>.Context context)
                throws IOException, InterruptedException {
            final String taskID = "Task " + inputSplit.getTaskNumber() + "/" +
                    inputSplit.getTaskCount() + " ";
            InStreamHandler.StringHandler progressAndSysout = new InStreamHandler.StringHandler() {
                public void handle(String s) {
                    s = taskID + s;
                    System.out.println(s);
                    context.setStatus(s);
                    context.progress();
                }
            };
            ProcessWorker processWorker = null;

            try {
                processWorker = startChildProcess(inputSplit, progressAndSysout);

                progressAndSysout.handle("WAIT FOR SUB-PROCESS TO DIE...");
                context.progress();
                int returncode = processWorker.waitFor();
                progressAndSysout
                        .handle("WAIT FOR SUB-PROCESS TO DIE...DONE. Return code:" +
                                returncode);
                context.progress();

                if ((taskFailOnNonZeroReturncode) && (returncode != 0)) {
                    throw new NonZeroReturnCodeException(
                            "Return code of subprocess was !=0: " + returncode);
                }
            } catch (Exception e) {
                throw createDebugException(inputSplit, context, taskID, e);
            } finally {
                if (processWorker != null) {
                    processWorker.destroyProcess();
                }
            }
        }


        private ArrayList<String> createStartupArgs(DistributedStartupInputSplit conf) {
            ArrayList<String> startupArgs = new ArrayList();
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.contains("HadoopClusterExec.tasks")) {
                    arg = arg.replaceAll("HadoopClusterExec.tasks",
                            Long.toString(conf.getTaskCount()));
                }
                if (arg.contains("HadoopClusterExec.taskNumber")) {
                    arg = arg.replaceAll("HadoopClusterExec.taskNumber",
                            Long.toString(conf.getTaskNumber()));
                }
                if (arg.contains("HadoopClusterExec.parentJVMArgs")) {
                    arg = arg.replaceAll("HadoopClusterExec.parentJVMArgs", thisVM_jvm_args);
                }
                if (arg.contains("HadoopClusterExec.child.JMX.bytes")) {
                    System.gc();
                    long max = Runtime.getRuntime().maxMemory();
                    long total = Runtime.getRuntime().totalMemory();
                    long free = max - total;
                    long childMaxMem = Double.valueOf((free * childMemRatio)).longValue();
                    String childMaxMemString = Long.toString(childMaxMem);

                    arg = arg.replaceAll("HadoopClusterExec.child.JMX.bytes", childMaxMemString);
                }
                startupArgs.add(arg);
            }

            return startupArgs;
        }

        private ProcessWorker startChildProcess(DistributedStartupInputSplit conf, InStreamHandler.StringHandler proccessStdOutHandler)
                throws IOException {
            ArrayList<String> startupArgs = createStartupArgs(conf);

            if (debugExceptionMessages) {
                throw new IOException("HadoopClusterExec.DEBUG_EXCEPTION_MESSAGES");
            }
            return createProcessWorker(startupArgs, cwd, proccessStdOutHandler);
        }


        private static ProcessWorker createProcessWorker(List<String> startupArgs, String cwd, InStreamHandler.StringHandler proccessStdOutHandler)
                throws IOException {
            ProcessBuilder processBuilder = new ProcessBuilder(startupArgs);

            if (cwd != null) {
                processBuilder.directory(new File(cwd));
            }
            proccessStdOutHandler.handle("Start child process with: " + startupArgs);
            Process process = processBuilder.start();
            proccessStdOutHandler.handle("PROCESS STARTED");

            ProcessWorker processWorker = new ProcessWorker(process, (String) startupArgs.get(0), proccessStdOutHandler, proccessStdOutHandler);
            processWorker.start();
            return processWorker;
        }


        private IOException createDebugException(DistributedStartupInputSplit inputSplit, Mapper<DistributedStartupInputSplit, NullWritable, Text, Text>.Context context, String taskID, Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String debugMsg = createDebugMsg(taskID, "ERROR", e.getMessage() +
                    "StackTrace:\n" + sw.toString(), inputSplit, context);
            IOException throwMe = new IOException(debugMsg, e);
            throwMe.setStackTrace(e.getStackTrace());
            return throwMe;
        }


        private String createDebugMsg(String taskID, String type, String message, DistributedStartupInputSplit conf, Mapper.Context context) {
            ArrayList<String> command = createStartupArgs(conf);
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            File processBuilderCWDDir = processBuilder.directory();
            File baseCWD = new File(".");
            File userCwdDir = cwd != null ? new File(cwd) :
                    processBuilderCWDDir;
            userCwdDir = userCwdDir == null ? baseCWD : userCwdDir;

            return "################# " +
                    type +
                    "  " +
                    taskID +
                    "###########################" + (
                    message == null ? "" : new StringBuilder("\nMessage:").append(message).toString()) +
                    "\nDEBUG INFORMATION:" +
                    "\nAdditional information (e.g. the console output of the subprocess) may be available in the task logs of this job." +
                    "\n" +
                    "\n Tried starting a process with:" +
                    "\n==========Exec ARGS=================\n" +
                    command +
                    "\n==========Node Distributed Cache====\n" +
                    HadoopClusterExec.getDistributedCacheContentsAsString(context) +
                    "\n==========Cache working directory=====\n user specified CWD: " +
                    userCwdDir.getAbsolutePath() +
                    "\n default ProcessBuilder CWD: " + (
                    processBuilderCWDDir == null ? "-none-" :
                            processBuilderCWDDir.getAbsolutePath()) +
                    "\n default CWD new File(\".\"): " +
                    baseCWD.getAbsolutePath() +
                    "\n mapred.output.dir=" +
                    context.getConfiguration().get("mapred.output.dir") +
                    "\n job.local.dir=" +
                    context.getConfiguration().get("job.local.dir") +

                    "\n==========CWD content=========\n" +
                    getCWDFileListWithJava(baseCWD) +

                    "\n################# /" + type + "  " + taskID +
                    "###########################\n";
        }


        private String getCWDFileList(File baseCWD) {
            return getCWDFileListWithJava(baseCWD);
        }

        private String getCWDFileListWithJava(File baseCWD) {
            String baseCWD_absolute = baseCWD.getAbsolutePath();
            final StringBuilder cwdContent = new StringBuilder();
            cwdContent.append("Absolute path of CWD:\n" + baseCWD_absolute)
                    .append("\n");
            try {
                listFiles(baseCWD, new FileFilter() {

                    public boolean accept(File pathname) {

                        cwdContent.append(DistributedExecMapper.getPermissions(pathname)).append(" ").append(DistributedExecMapper.getDateTime(pathname)).append(" ");
                        cwdContent
                                .append(pathname.getPath())
                                .append(pathname.isDirectory() ? File.separator :
                                        "").append("\n");
                        return true;
                    }
                }, true);
            } catch (Exception e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));

                cwdContent.append("\n Error while printing cwd contents: " +
                        e.getMessage() + "\n" + sw.toString());
            }
            return cwdContent.toString();
        }

        private String getCWDFileListWithExec_(String baseCWD) {
            boolean isUnix = File.separatorChar == '/';
            final StringBuilder sb = new StringBuilder();
            if (isUnix) {
                InStreamHandler.StringHandler sh = new InStreamHandler.StringHandler() {
                    public void handle(String s) {
                        sb.append(s).append("\n");
                    }
                };
                ProcessWorker process = null;


                try {
                    process = createProcessWorker(Arrays.asList(new String[]{"ls", "-alhHLR"}), baseCWD, sh);
                    int i = process.waitFor();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    if (process != null) {
                        process.destroyProcess();
                    }
                }
            }

            return sb.toString();
        }


        protected static final ThreadLocal<Calendar> calendar = new ThreadLocal() {
            protected Calendar initialValue() {
                return Calendar.getInstance();
            }
        };

        private static String getDateTime(File file) {
            Calendar cal = (Calendar) calendar.get();
            cal.setTimeInMillis(System.currentTimeMillis());
            int curYear = cal.get(1);
            cal.setTimeInMillis(file.lastModified());
            int fileYear = cal.get(1);
            String sM = cal.getDisplayName(2,
                    1, Locale.getDefault());

            int sD = cal.get(5);
            if (curYear == fileYear) {
                int sHou = cal.get(11);
                int sMin = cal.get(12);

                return String.format("%3s %2s %02d:%02d", new Object[]{sM, Integer.valueOf(sD), Integer.valueOf(sHou), Integer.valueOf(sMin)});
            }
            String sY = Integer.toString(fileYear);


            return String.format("%3s %2s %d", new Object[]{sM, Integer.valueOf(sD), Integer.valueOf(fileYear)});
        }


        private static String getPermissions(File pathname) {
            boolean r = pathname.canRead();
            boolean w = pathname.canWrite();
            boolean x = pathname.canExecute();
            return (pathname.isDirectory() ? "d" : "-") + (r ? 'r' : '-') + (
                    w ? 'w' : '-') + (x ? 'x' : '-');
        }


        private List<File> listFiles(File cwdDir, FileFilter fileFilter, boolean addDirFileToResult) {
            ArrayList<File> returnFiles = new ArrayList();
            if (cwdDir == null) {
                return returnFiles;
            }
            File[] files = cwdDir.listFiles();

            if (files != null) {
                Arrays.sort(files);
                for (File file : files) {
                    if (fileFilter.accept(file)) {
                        if (file.isFile()) {
                            returnFiles.add(file);
                        } else if (file.isDirectory()) {
                            if (addDirFileToResult) {
                                returnFiles.add(file);
                            }
                            returnFiles.addAll(listFiles(file, fileFilter,
                                    addDirFileToResult));
                        }
                    }
                }
            }
            return returnFiles;
        }


        public static class InStreamHandler
                extends Thread {
            private String PRE = null;
            private InputStream is;
            private boolean transferClose;
            private StringHandler sh;

            public InStreamHandler(InputStream is, String pre, boolean transferClose, StringHandler sh) {
                setDaemon(true);
                this.is = is;
                PRE = pre;
                this.transferClose = transferClose;
                this.sh = sh;
            }

            public void run() {
                System.out.println("InStreamHandler '" + PRE + "' started");
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(is));
                try {
                    String line;
                    while ((line = in.readLine()) != null) {
                        sh.handle(PRE + line);
                    }
                } catch (Exception e) {
                    HadoopClusterExec.LOG.error(PRE, e);

                    if (transferClose) {
                        try {
                            in.close();
                        } catch (IOException localIOException) {
                        }
                    }
                } finally {
                    if (transferClose) {
                        try {
                            in.close();
                        } catch (IOException localIOException1) {
                        }
                    }
                }
                System.out.println("InStreamHandler '" + PRE + "' closed");
            }

            public static abstract interface StringHandler {
                public abstract void handle(String paramString);
            }
        }

        private static class ProcessWorker extends Thread {
            private final Process process;
            private volatile Integer exit;
            private volatile boolean _isAlive;

            private ProcessWorker(Process process, String name, InStreamHandler.StringHandler stdout, InStreamHandler.StringHandler stderr) {
                super();
                this.process = process;
                setDaemon(true);

                InStreamHandler processSysout = new InStreamHandler(
                        process.getInputStream(), name + ">", false, stdout);
                processSysout.start();
                InStreamHandler processErr = new InStreamHandler(
                        process.getErrorStream(), name + "_err>", false, stderr);
                processErr.start();

                final ProcessWorker me = this;
                Thread closeChildThread = new Thread() {
                    public void run() {
                        me.destroyProcess();
                    }

                };
                Runtime.getRuntime().addShutdownHook(closeChildThread);
            }

            public int waitFor() throws InterruptedException {
                return process.waitFor();
            }

            public synchronized void destroyProcess() {
                if (_isAlive) {
                    System.out.println("DESTROY PROCESS..");
                    closeStreams();
                    process.destroy();
                    _isAlive = false;
                    System.out.println("DESTROY PROCESS..DONE");
                }
            }


            private void closeStreams() {
                silentClose(process.getOutputStream());
                silentClose(process.getErrorStream());
                silentClose(process.getInputStream());
            }

            private static void silentClose(Closeable c) {
                try {
                    c.close();
                } catch (Exception localException) {
                }
            }

            public void run() {
                _isAlive = true;
                try {
                    exit = Integer.valueOf(process.waitFor());
                } catch (InterruptedException localInterruptedException) {
                } finally {
                    destroyProcess();
                }
            }


            public boolean isProcessAlive() {
                return _isAlive;
            }


            public Integer getProcessExitCode() {
                return exit;
            }
        }
    }

    public static class MultiOutputStream {
        private InputStream src;
        private final CopyOnWriteArrayList<OutputStream> outStreams = new CopyOnWriteArrayList();
        private AtomicBoolean isOpen = new AtomicBoolean(false);
        private Thread readerThread;

        public MultiOutputStream(final InputStream src, OutputStream... out) {
            this.src = src;
            if (out != null) {
                outStreams.addAll(Arrays.asList(out));
            }
            readerThread = new Thread(new Runnable() {
                public void run() {
                    isOpen.set(true);
                    try {
                        byte[] buffer = new byte['Ѐ'];
                        for (int n = 0; n != -1; n = src.read(buffer)) {

                            for (OutputStream dest : outStreams) {
                                dest.write(buffer, 0, n);
                                dest.flush();
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        isOpen.set(false);
                    }

                }
            }, "MultiOutputStream_ReaderThread");
            readerThread.setDaemon(true);
            readerThread.start();
        }

        public void addOutputStream(OutputStream os) throws IOException {
            if (!isOpen.get()) {
                throw new IOException("Inputstream is closed");
            }
            outStreams.add(os);
        }

        public void removeOutputStream(OutputStream os) throws IOException {
            if (!isOpen.get()) {
                throw new IOException("Inputstream is closed");
            }
            outStreams.remove(os);
        }

        public boolean isOpen() {
            return isOpen.get();
        }

        public void close() throws IOException {
            readerThread.interrupt();
            try {
                readerThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            src.close();
        }
    }

    public static class ToolBox {
        public ToolBox() {
        }

        public static String argsToString(String[] arg0) {
            StringBuilder tmp = new StringBuilder();
            for (int i = 0; i < arg0.length; i++) {
                tmp.append(arg0[i]);
            }
            String joinChar = getJoinChar(tmp.toString());
            StringBuilder sb = new StringBuilder();
            sb.append(joinChar);
            for (int i = 0; ; i++) {
                sb.append(arg0[i]);
                if (i == arg0.length - 1) {
                    return sb.toString();
                }
                sb.append(joinChar);
            }
        }

        public static String[] argsFromString(String arg0) {
            String splitChar = String.valueOf(arg0.charAt(0));
            String[] splits = arg0.substring(1).split(splitChar);
            return splits;
        }

        public static String[] argsFromStringEscaped(String arg0) {
            String splitChar = String.valueOf(arg0.charAt(0));
            String[] splits = arg0.substring(1).split(splitChar);
            for (int i = 0; i < splits.length; i++) {
                splits[i] = ("\"" + splits[i] + "\"");
            }
            return splits;
        }


        private static final String[] joinChars = {" ", "$", "&", ";", "_",
                "["};

        public static String getJoinChar(String in) {
            for (int i = 0; i < joinChars.length; i++) {
                if (!in.contains(joinChars[i])) {
                    return joinChars[i];
                }
            }

            for (int i = 0; i < 255; i++) {
                if (!in.contains(String.valueOf((char) i))) {
                    return String.valueOf((char) i);
                }
            }
            return null;
        }
    }
}
