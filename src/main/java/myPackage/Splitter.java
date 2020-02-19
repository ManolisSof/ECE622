package myPackage;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.akka.org.jboss.netty.util.DefaultObjectSizeEstimator;

public class Splitter implements MapFunction<String, Tuple5<String, String, Double, Double, Integer>>
{
    public Tuple5<String, String, Double, Double, Integer> map(String value) {
        String[] words = value.split(",");
        return new Tuple5<>(words[2], words[5], Double.parseDouble(words[6]), Math.pow(Double.parseDouble(words[6]),2), 1);
    }
}