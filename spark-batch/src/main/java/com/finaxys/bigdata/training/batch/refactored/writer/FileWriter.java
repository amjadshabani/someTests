package com.finaxys.bigdata.training.batch.refactored.writer;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;

public class FileWriter extends AbstractWriter {


    @Override
    public void write() {

    }

    @Override
    public void write(DataFrame df) {
        df.write().mode(SaveMode.Overwrite).format("text").save(config.getOutputFile());

    }

    @Override
    public void writeAsTable(DataFrame df) {
        df.write().mode(SaveMode.Overwrite).format("text").saveAsTable(config.getOutputTable());
    }
}
