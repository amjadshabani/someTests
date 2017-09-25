package com.finaxys.bigdata.training.batch.refactored;

import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * This class define project-wide configuration that is shared among all project unities<br/>
 * It should be serialized so even when copied to the executors the configuration will be all the same
 */
public class ProjectConfiguration implements Serializable {
    private static final Logger logger = Logger.getLogger(ProjectConfiguration.class);

    private static ProjectConfiguration ourInstance;
    private final String appName;
    private final String fileLocation;
    private final String configuration;
    private final String writerType;
    private final String outputTable;
    private final String outputFile;
    /**
     * We are hardcoding the configuration for the sake of simplicity
     * In real use case those configuration should be taken from outer source like a configuration file.
     */
    private ProjectConfiguration() {
        fileLocation = "spark-batch/src/main/resources/orders_data.csv";
        configuration = "";
        writerType = "orc";
        outputTable = "results";
        appName = "Test-Application";
        outputFile = "/tmp/orders_results";
    }

    public static synchronized ProjectConfiguration getInstance() {
        if (ourInstance == null) {
            return new ProjectConfiguration();
        } else {
            return ourInstance;

        }
    }

    public String getAppName() {
        return appName;
    }

    public String getFileLocation() {
        return fileLocation;
    }

    public String getConfiguration() {
        return configuration;
    }

    public String getWriterType() {
        return writerType;
    }

    public String getOutputTable() {
        return outputTable;
    }

    public String getOutputFile() {
        return outputFile;
    }

    @Override
    public String toString() {
        return "AppName: " + appName + ", fileLocation: " + fileLocation + ", writerType: " + writerType + ", outputFile: " + outputFile
                + ", outputTable: " + outputTable;
    }
}
