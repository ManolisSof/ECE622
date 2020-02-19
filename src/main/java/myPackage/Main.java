package myPackage;


import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;


public class Main {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set global parallelism
        env.setParallelism(3);
        // Read data from the file
        DataStream<String> data = env.readTextFile("openaqFull.csv");

//        DataStream<Tuple5<String, String, Double, Double, Integer>> mapped = data.map(new Splitter());
        DataStream<Tuple6<String, String, Double, Double, Double, Integer>> finalData = data
                .map(new Splitter()) // Split data and keep only relevant attributes
                .keyBy(new int[]{0, 1}) // groupBy country and parameter
                .reduce(new Reduce1()) // Reduce. New tuples will be at the format of Country, Parameter, Measurement, Measurement, Measurement, Count
                .map(new Mapper()); // Final mapping. New tuples will be at the format of Country, Parameter, Average, Standard Deviation, Gamma, Count

        // Now we have to try to add all gamma(i) and get the s(i) for each stratum.

//        finalData.print();
        // Delete output file if it already exists and create the output file.
        try {
            FileUtils.deleteDirectory(Paths.get("/home/manolis/Desktop/Special Topics on Databases/openaq data/GroupByCity_firstPass").toFile());
        }catch(NoSuchFileException e)
        {
            System.out.println("No such file/directory exists");
        }
        catch(IOException e)
        {
            System.out.println("Invalid permissions.");
        }

        finalData.writeAsCsv("/home/manolis/Desktop/Special Topics on Databases/openaq data/GroupByCity_firstPass");

        env.execute("Avg Job");

    }
}