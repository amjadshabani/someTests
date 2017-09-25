package com.finaxys.bigdata.training.batch.refactored;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * This class define project-wide configuration that is shared among all project unities<br/>
 * It should be serialized so even when copied to the executors the configuration will be all the same
 */
public class ProjectConfiguration implements Serializable {
    private static final Logger logger = Logger.getLogger(ProjectConfiguration.class);

    Options options = new Options();


    private static ProjectConfiguration ourInstance;


    private final String appName;
    private final String fileLocation;
    private final String configuration;
    private final String writerType;
    private final String outputTable;
    private final String outputFile;

    /**
     * We are hardcoding the configuration for the sake of simplicity
     * In real use case those configuration should be taken from outer source like a configuration file
     * .
     */
    private ProjectConfiguration(String [] args) throws ParseException {
        options.addRequiredOption("i","input", true, "Location of the input file");
        options.addOption("w","writer", true, "Writer type: supported are orc, file");
        options.addOption("t", "table", true , "Output table to store results");
        options.addRequiredOption("a", "app", true, "Spark Application Name");
        options.addOption("o","output", true, "Location of the output file on which the results will be saved");

        CommandLineParser cliParser = new DefaultParser();
        CommandLine cli;

            cli = cliParser.parse(options,args);
            fileLocation = cli.getOptionValue("i" );
            writerType = cli.getOptionValue("w", "file");
            outputTable = cli.getOptionValue("t", "results");
            appName = cli.getOptionValue("a");
            outputFile = cli.getOptionValue("o", "/tmp/orders_results");

//        fileLocation = "spark-batch/src/main/resources/orders_data.csv";
        configuration = "";
//        writerType = "orc";
//        outputTable = "results";
//        appName = "Test-Application";
//        outputFile = "/tmp/orders_results";
    }

    public static synchronized ProjectConfiguration getInstance(String[] args) throws ParseException {
        if (ourInstance == null) {
                return new ProjectConfiguration(args);
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
