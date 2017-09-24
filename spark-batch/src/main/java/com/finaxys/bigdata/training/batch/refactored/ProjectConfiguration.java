package com.finaxys.bigdata.training.batch.refactored;

public class ProjectConfiguration {
    private static ProjectConfiguration ourInstance = new ProjectConfiguration();

    public static ProjectConfiguration getInstance() {
        return ourInstance;
    }


    private String fileLocation;
    private String configuration;



    private ProjectConfiguration() {
    }
}
