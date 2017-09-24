package com.finaxys.bigdata.training.batch.refactored.writer;

import com.finaxys.bigdata.training.batch.refactored.ProjectConfiguration;
import org.apache.spark.sql.DataFrame;

import java.io.Serializable;

public abstract class AbstractWriter implements Serializable {

    ProjectConfiguration config = ProjectConfiguration.getInstance();

    /**
     * write results from rdd to a destination defined in the implementation classes
     */
    abstract public void write();

    /**
     * Writing a dataframe to file type defined in the implementation class
     *
     * @param df
     */
    abstract public void write(DataFrame df);

    /**
     * Write the dataframe to a table
     *
     * @param df
     */
    abstract public void writeAsTable(DataFrame df);

}
