package myPackage;

import org.apache.commons.math3.util.Precision;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

public class Mapper implements MapFunction<Tuple5<String, String, Double, Double, Integer>, Tuple6<String, String, Double, Double, Double, Integer>> {

    public Tuple6<String, String, Double, Double, Double, Integer> map(Tuple5<String, String, Double, Double, Integer> value) throws Exception {
        Double avg = value.f2 / value.f4;
        Double variance = value.f3 / value.f4 - Math.pow(avg, 2);
        Double std = Math.pow(variance, 0.5);
        Double gamma = variance / avg;
        return new Tuple6<>(value.f0, value.f1, Precision.round(avg, 4), Precision.round(std, 4), Precision.round(gamma, 4), value.f4);
    }
}
