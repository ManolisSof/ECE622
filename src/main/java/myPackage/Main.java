package myPackage;


import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Properties;


public class Main {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set global parallelism
//        env.setParallelism(3);

        // Create a Flink-Kafka Producer to pass data from a topic to flink.
        // Set configurations of the producer
        String bootsrtapServers = "127.0.0.1:9092";
        String topic = "flink_first_pass";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrtapServers);
        // Add the kafka topic declared above as data source.
        DataStream<String> kafkaData = env.addSource(new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties));


        // Alternatively read data from the file (uncomment to read from file)
//        DataStream<String> data = env.readTextFile("openaqFull.csv");

//        DataStream<Tuple5<String, String, Double, Double, Integer>> mapped = data.map(new Splitter());
        DataStream<Tuple6<String, String, Double, Double, Double, Integer>> avgVarData = kafkaData
                .map(new Splitter()) // Split data and keep only relevant attributes
                .keyBy(new int[]{0, 1}) // groupBy country and parameter
                .reduce(new Reduce1()) // Reduce. New tuples will be at the format of Country, Parameter, Measurement, Measurement, Measurement, Count
                .map(new Mapper()); // Final mapping. New tuples will be at the format of Country, Parameter, Average, Standard Deviation, Gamma, Count

        // Now we have to try to add all gamma(i) and get the s(i) for each stratum.

        avgVarData.print();
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

        // Create a csv file as a data Sink to print results.
//        avgVarData.writeAsCsv("/home/manolis/Desktop/Special Topics on Databases/openaq data/GroupByCity_firstPass");

        // Execute job
        env.execute("First Pass Job");

    }
}