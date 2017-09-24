package com.finaxys.bigdata.training.batch.refactored;

import java.io.Serializable;

/**
 * This class define project-wide configuration that is shared among all project unities<br/>
 * It should be serialized so even when copied to the executors the configuration will be all the same
 */
public class ProjectConfiguration implements Serializable {
    private static ProjectConfiguration ourInstance;

    public static synchronized ProjectConfiguration getInstance() {
        if (ourInstance == null) {
            return ourInstance;
        } else {
            return new ProjectConfiguration();
        }
    }

    public final String fileLocation;
    public final String configuration;
    public final String writer;
    public final String outputTable;


    /**
     * We are hardcoding the configuration for the sake of simplicity
     * In real use case those configuration should be taken from outer source like a configuration file.
     */
    private ProjectConfiguration() {
        fileLocation = "/tmp/orders.csv";
        configuration = "";
        writer = "orc";
        outputTable = "results";
    }
}
