package fr.iiil.bigdata.reminder.functions.reader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class TextFileReader implements Supplier<Dataset<String>> {
    private final String inputPathStr;
    private final SparkSession sparkSession;
    private final FileSystem hdfs;

    public TextFileReader(String inputPathStr) throws IOException {
        this(inputPathStr, SparkSession.builder().config(new SparkConf().setMaster("local[2]").setAppName("AppSpark")).getOrCreate(), FileSystem.get(new Configuration()));
    }

    @Override
    public Dataset<String> get() {
        log.info("reading file at inputPathStr={}", inputPathStr);
        Dataset<String> lines = sparkSession.emptyDataset(Encoders.STRING());
        try {
            if (hdfs.exists(new Path(inputPathStr))) {
                lines = sparkSession.read().textFile(inputPathStr);
            }
            return lines;
        } catch (Exception exception) {
            log.error("failed to read file at inputPathStr={} due to ...", inputPathStr, exception);
        }
        return lines;
    }
}
