package com.finaxys.bigdata.training.batch.refactored.writer;


import com.finaxys.bigdata.training.batch.refactored.ProjectConfiguration;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;

import java.io.Serializable;

public class OrcWriter extends AbstractWriter implements Serializable {

    @Override
    public void write() {

    }

    /**
     * Write the dataframe into ORC file
     *
     * @param df
     */
    @Override
    public void write(DataFrame df, String outputFile) {
        df.write().mode(SaveMode.Overwrite).format("orc").save(outputFile);
    }

    @Override
    public void writeAsTable(DataFrame df, String outputTable) {
        df.write().mode(SaveMode.Overwrite).format("orc").saveAsTable(outputTable);
    }
}
