package fr.iiil.bigdata.reminder;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.iiil.bigdata.reminder.bean.ActeDeces;
import fr.iiil.bigdata.reminder.functions.func.DecesParJour;
import fr.iiil.bigdata.reminder.functions.mapper.ConvertLinesToActeDeces;
import fr.iiil.bigdata.reminder.functions.reader.TextFileReader;
import fr.iiil.bigdata.reminder.functions.receiver.KafkaReceiver;
import fr.iiil.bigdata.reminder.functions.writer.ResultWriterCollections;
import fr.iiil.bigdata.reminder.functions.writer.ResultWriterHbase;
import fr.iiil.bigdata.reminder.functions.writer.ResultWriterRow;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.util.List;

/**
 * Hello world!
 *
 */

public class KafkaActeDecesApp {
    public static void main( String[] args ) throws IOException {
        Config config = ConfigFactory.load();
        String inputPathStr = config.getString("3il.path.input");
        String outputPathStr = config.getString("3il.path.output");
        String masterUrl = config.getString("3il.spark.master");
        String checkpointPathStr = config.getString("3il.path.checkpoint");
        List<String> topics = config.getStringList("3il.spark.topics");

        System.out.println( "Hello Java Collections!" );

        SparkConf sparkConf = new SparkConf().setMaster(masterUrl).setAppName("SparkApp");
        final SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        final JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate(
                checkpointPathStr,
                () -> new JavaStreamingContext(sparkSession.sparkContext().getConf(), new Duration(10000)),
                sparkSession.sparkContext().hadoopConfiguration()
        );

        KafkaReceiver kafkaReceiver = new KafkaReceiver(topics, javaStreamingContext);
        JavaDStream<String> inputStream = kafkaReceiver.get();
        inputStream.foreachRDD(
                new VoidFunction2<JavaRDD<String>, Time>() {
                    @Override
                    public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                        Dataset<Row> inputDS = sparkSession.createDataFrame(stringJavaRDD, Row.class);

                        DecesParJour decesParJour = new DecesParJour();
                        ResultWriterCollections resultWriterCollections = new ResultWriterCollections(outputPathStr);
                        ResultWriterRow resultWriterRow = new ResultWriterRow(outputPathStr);
                        ConvertLinesToActeDeces convertLinesToActeDeces = new ConvertLinesToActeDeces();

//                        Dataset<String> lines = reader.get();
//
//                        Dataset<ActeDeces> actesDeces = convertLinesToActeDeces.apply(lines);
//                        Dataset<Row> acteDecesParJour = decesParJour.apply(actesDeces);
//
//                        resultWriterRow.accept(acteDecesParJour);
//                        resultWriterCollections.accept(actesDeces);
                    }
                }
        );

    }
}
