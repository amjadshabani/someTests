package com.finaxys.bigdata.training.batch.refactored.writer;

import org.apache.spark.sql.DataFrame;

import java.io.Serializable;

public abstract class AbstractWriter implements Serializable {
    abstract public void write();
    abstract public void write(DataFrame df);

}
