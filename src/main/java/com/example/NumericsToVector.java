package com.example;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.ml.linalg.DenseVector;

public class NumericsToVector extends ScalarFunction {

    @DataTypeHint(value = "RAW", bridgedTo = DenseVector.class)
    public DenseVector eval(Double... fields) {
        var target = new double[fields.length];
        for (int i = 0; i < target.length; i++) {
            target[i] = fields[i];
        }
        return new DenseVector(target);
    }
}
