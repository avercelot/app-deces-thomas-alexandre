package fr.iiil.bigdata.actedeces.functions.reader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class TextFileReader implements Supplier<Dataset<String>> {
    private final String inputPathStr;
    private final SparkSession sparkSession;

    public TextFileReader(String inputPathStr) {
        this(inputPathStr, SparkSession.builder().config(new SparkConf().setMaster("local[2]").setAppName("AppSpark")).getOrCreate());
    }

    @Override
    public Dataset<String> get() {
        log.info("reading file at inputPathStr={}", inputPathStr);
        try {
            Dataset<String> lines = sparkSession.read().textFile(inputPathStr);
            return lines;
        } catch (Exception exception) {
            log.error("failed to read file at inputPathStr={} due to ...", inputPathStr, exception);
        }
        return sparkSession.emptyDataset(Encoders.STRING());
    }
}
