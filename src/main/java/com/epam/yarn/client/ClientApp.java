package com.epam.yarn.client;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

import static com.epam.yarn.client.ClientOptions.*;
import static com.epam.yarn.Constants.*;
import static org.apache.hadoop.yarn.api.ApplicationConstants.CLASS_PATH_SEPARATOR;
import static org.apache.hadoop.yarn.api.ApplicationConstants.LOG_DIR_EXPANSION_VAR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.YARN_APPLICATION_CLASSPATH;

public class ClientApp {

    private static final Log LOG = LogFactory.getLog(ClientApp.class);

    private final long clientStartTime = System.currentTimeMillis();

    private YarnClient yarnClient;

    private Configuration conf;

    private String appName;

    /* App master priority */
    private int amPriority;

    /* Queue for App master */
    private String amQueue;

    /* Amt. of allocated memory for App master (megabytes) */
    private long amMemory;

    /* Amt. of allocated virtual cores for App master */
    private int amVCores;

    private String appMasterJarPath;

    /* Amt of memory to request for container in which the HelloYarn will be executed */
    private int containerMemory;

    /* Amt. of virtual cores to request for container in which the HelloYarn will be executed */
    private int containerVirtualCores;

    /* No. of containers in which the HelloYarn needs to be executed */
    private int numContainers;

    /* Timeout threshold for client. Kill app after time interval expires. */
    private long clientTimeout;

    /* Command line options */
    private Options opts;

    public ClientApp() {
        createYarnClient();
        initOptions();
    }

    private void createYarnClient() {
        yarnClient = YarnClient.createYarnClient();
        conf = new YarnConfiguration();
        yarnClient.init(conf);
    }

    private void initOptions() {
        opts = new Options();
        opts.addOption(APP_NAME.getName(), true, "Application Name. Default value - HelloYarn");
        opts.addOption(PRIORITY.getName(), true, "Application Priority. Default 0");
        opts.addOption(QUEUE.getName(), true, "RM Queue in which this application is to be submitted");
        opts.addOption(TIMEOUT.getName(), true, "Application timeout in milliseconds");
        opts.addOption(MASTER_MEMORY.getName(), true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption(MASTER_VCORES.getName(), true, "Amount of virtual cores to be requested to run the application master");
        opts.addOption(JAR.getName(), true, "Jar file containing the application master");
        opts.addOption(CONTAINER_MEMORY.getName(), true, "Amount of memory in MB to be requested to run the HelloYarn");
        opts.addOption(CONTAINER_VCORES.getName(), true, "Amount of virtual cores to be requested to run the HelloYarn");
        opts.addOption(NUM_CONTAINERS.getName(), true, "No. of containers on which the HelloYarn needs to be executed");
        opts.addOption(HELP.getName(), false, "Print usage");
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }

    public boolean init(String[] args) throws ParseException {
        CommandLine cliParser = new GnuParser().parse(opts, args);


        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for client to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        appName = cliParser.getOptionValue(APP_NAME.getName(), APP_NAME.getDefault());
        amPriority = Integer.parseInt(cliParser.getOptionValue(PRIORITY.getName(), PRIORITY.getDefault()));
        amQueue = cliParser.getOptionValue(QUEUE.getName(), QUEUE.getDefault());
        amMemory = Long.parseLong(cliParser.getOptionValue(MASTER_MEMORY.getName(),MASTER_MEMORY.getDefault()));
        amVCores = Integer.parseInt(cliParser.getOptionValue(MASTER_VCORES.getName(), MASTER_MEMORY.getDefault()));

        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }
        if (amVCores < 0) {
            throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                    + " Specified virtual cores=" + amVCores);
        }

        if (!cliParser.hasOption(JAR.getName())) {
            throw new IllegalArgumentException("No jar file specified for application master");
        }

        appMasterJarPath = cliParser.getOptionValue(JAR.getName());
        containerMemory = Integer.parseInt(cliParser.getOptionValue(CONTAINER_MEMORY.getName(), CONTAINER_MEMORY.getDefault()));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(CONTAINER_VCORES.getName(), CONTAINER_VCORES.getDefault()));
        numContainers = Integer.parseInt(cliParser.getOptionValue(NUM_CONTAINERS.getName(), NUM_CONTAINERS.getDefault()));

        if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
                    + " exiting."
                    + " Specified containerMemory=" + containerMemory
                    + ", containerVirtualCores=" + containerVirtualCores
                    + ", numContainer=" + numContainers);
        }

        clientTimeout = Integer.parseInt(cliParser.getOptionValue(TIMEOUT.getName(), TIMEOUT.getDefault()));

        return true;
    }

    /**
     * Main run function for the client
     * @return true if application completed successfully
     * @throws java.io.IOException
     * @throws org.apache.hadoop.yarn.exceptions.YarnException
     */
    public boolean run() throws IOException, YarnException {
        LOG.info("Running Client");
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        // A resource ask cannot exceed the max.
        checkResources(appResponse);

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setApplicationName(appName);

        // Set up resource type requirements
        // For now, both memory and vcores are supported, so we set memory and
        // vcores requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(amMemory);
        capability.setVirtualCores(amVCores);
        appContext.setResource(capability);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(amPriority);
        appContext.setPriority(priority);

        appContext.setQueue(amQueue);
        appContext.setAMContainerSpec(getAMContainerSpec(appId.getId()));

        LOG.info("Submitting application to ASM");
        yarnClient.submitApplication(appContext);

        return monitorApplication(appId);
    }


    private void checkResources(GetNewApplicationResponse appResponse) {

        long maxMem = appResponse.getMaximumResourceCapability().getMemorySize();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory
                    + ", max=" + maxMem);
            amMemory = maxMem;
        }

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

        if (amVCores > maxVCores) {
            LOG.info("AM virtual cores specified above max threshold of cluster. "
                    + "Using max value." + ", specified=" + amVCores
                    + ", max=" + maxVCores);
            amVCores = maxVCores;
        }
    }


    private ContainerLaunchContext getAMContainerSpec(int appId) throws IOException {

        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        FileSystem fs = FileSystem.get(conf);

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        addToLocalResources(fs, appMasterJarPath, AM_JAR_NAME, appId, localResources, null);
        amContainer.setLocalResources(localResources);

        LOG.info("Set the environment for the application master");
        amContainer.setEnvironment(getAMEnvironment(localResources, fs));

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<>();
        LOG.info("Setting up app master command");
        vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("-Xmx" + amMemory + "m");
        vargs.add("com.epam.yarn.master.MasterApp");
        vargs.add("--container_memory " + containerMemory);
        vargs.add("--container_vcores " + containerVirtualCores);
        vargs.add("--num_containers " + numContainers);
        vargs.add("--priority 0");
        vargs.add("1>" + LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<>();
        commands.add(command.toString());
        amContainer.setCommands(commands);

        return amContainer;
    }


    private void addToLocalResources(FileSystem fs,
                                     String fileSrcPath,
                                     String fileDstName,
                                     int appId,
                                     Map<String, LocalResource> localResources,
                                     String resources)
            throws IOException {

        String suffix = appName + "/" + appId + "/" + fileDstName;
        Path dst = new Path(fs.getHomeDirectory(), suffix);

        if (StringUtils.isEmpty(fileSrcPath)) {
            try (FSDataOutputStream out = FileSystem.create(fs, dst, new FsPermission((short)0710))) {
                out.writeUTF(resources);
            }
        }
        else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }

        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        URL.fromURI(dst.toUri()),
                        LocalResourceType.FILE,
                        LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(),
                        scFileStatus.getModificationTime());
        localResources.put(fileDstName, scRsrc);
    }


    private Map<String, String> getAMEnvironment(Map<String, LocalResource> localResources,
                                                 FileSystem fs)
            throws IOException{
        Map<String, String> env = new HashMap<>();

        LocalResource appJarResource = localResources.get(AM_JAR_NAME);
        Path hdfsAppJarPath = new Path(fs.getHomeDirectory(), appJarResource.getResource().getFile());
        FileStatus hdfsAppJarStatus = fs.getFileStatus(hdfsAppJarPath);

        long hdfsAppJarLength = hdfsAppJarStatus.getLen();
        long hdfsAppJarTimestamp = hdfsAppJarStatus.getModificationTime();

        env.put(AM_JAR_PATH, hdfsAppJarPath.toString());
        env.put(AM_JAR_TIMESTAMP, Long.toString(hdfsAppJarTimestamp));
        env.put(AM_JAR_LENGTH, Long.toString(hdfsAppJarLength));

        // Add AppMaster.jar location to classpath
        // At some point we should not be required to add
        // the hadoop specific classpaths to the env.
        // It should be provided out of the box.
        // For now setting all required classpaths including
        // the classpath to "." for the application jar
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                .append(CLASS_PATH_SEPARATOR)
                .append("./*");

        for (String c : conf.getStrings(
                YARN_APPLICATION_CLASSPATH,
                DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        env.put("CLASSPATH", classPathEnv.toString());

        return env;
    }


    /**
     * Monitor the submitted application for completion.
     * Kill application if time expires.
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws org.apache.hadoop.yarn.exceptions.YarnException
     * @throws java.io.IOException
     */
    private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {

        while (true) {
            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("Thread sleep in monitoring loop interrupted");
            }

            ApplicationReport report = yarnClient.getApplicationReport(appId);
            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();

            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. "
                            + " Breaking monitoring loop : ApplicationId:" + appId.getId());
                    return true;
                }
                else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                    return false;
                }
            }
            else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                return false;
            }

            if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
                LOG.info("Reached client specified timeout for application. Killing application"
                        + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                forceKillApplication(appId);
                return false;
            }
        }
    }

    /**
     * Kill a submitted application by sending a call to the ASM
     * @param appId Application Id to be killed.
     * @throws org.apache.hadoop.yarn.exceptions.YarnException
     * @throws java.io.IOException
     */
    private void forceKillApplication(ApplicationId appId)
            throws YarnException, IOException {
        yarnClient.killApplication(appId);
    }

    /**
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            ClientApp client = new ClientApp();
            LOG.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(-1);
            }
            result = client.run();
        } catch (Throwable t) {
            LOG.fatal("Error running Client", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(2);
    }
}
