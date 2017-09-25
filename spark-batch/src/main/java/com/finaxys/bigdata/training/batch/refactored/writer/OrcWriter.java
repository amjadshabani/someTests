package com.finaxys.bigdata.training.batch.refactored.writer;


import com.finaxys.bigdata.training.batch.refactored.ProjectConfiguration;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class OrcWriter extends AbstractWriter {
    private final ProjectConfiguration config = ProjectConfiguration.getInstance();

    @Override
    public void write() {

    }

    /**
     * Write the dataframe into ORC file
     *
     * @param df
     */
    @Override
    public void write(DataFrame df) {
        df.write().mode(SaveMode.Overwrite).format("orc").save(config.getOutputFile());
    }

    @Override
    public void writeAsTable(DataFrame df) {
        df.write().mode(SaveMode.Overwrite).format("orc").saveAsTable(config.getOutputTable());
    }
}
