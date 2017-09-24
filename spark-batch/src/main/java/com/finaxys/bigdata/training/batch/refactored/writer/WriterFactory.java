package com.finaxys.bigdata.training.batch.refactored.writer;

import com.finaxys.bigdata.training.batch.refactored.ProjectConfiguration;

public class WriterFactory {

    ProjectConfiguration config = ProjectConfiguration.getInstance();

    public AbstractWriter getWriter() {
        String writerType = config.writer;
        if (writerType.toLowerCase().equals("orc")) {
            return new OrcWriter();

        } else if (writerType.toLowerCase().equals("file")) {
            return new FileWriter();
        } else
            throw new IllegalArgumentException("Unsupported writer type: " + writerType);

    }
}
