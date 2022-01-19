package fr.iiil.bigdata.reminder.functions.writer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.iiil.bigdata.reminder.bean.ActeDeces;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class ResultWriterTest {
    private SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("ResultWriterTest").getOrCreate();
    private final String sampleLine = "BENAMEUR*HABIB/                                                                 11941020892352DEPARTEMENT D'ORAN                                          2021122401004262";
    Config config = ConfigFactory.load();
    String outputPathStr = config.getString("3il.path.output");

    @Test
    public void testWriterOneLine() {
        ResultWriterCollections resultWriterCollections = new ResultWriterCollections(outputPathStr);

        ActeDeces expected = ActeDeces.builder().nomPrenom("BENAMEUR*HABIB/")
                .build();
        Dataset<ActeDeces> actesDeces = sparkSession.createDataset(Collections.singletonList(expected), Encoders.bean(ActeDeces.class));
        resultWriterCollections.accept(actesDeces);

        Dataset<Row> tmpds = sparkSession.read().parquet(outputPathStr);
        tmpds.printSchema();
        Dataset<ActeDeces> actual = tmpds.as(Encoders.bean(ActeDeces.class));

        assertThat(actual.collectAsList()).containsExactlyInAnyOrder(expected);
    }

    @Test
    public void testWriterOneLineHdfs() {
        FileSystem hdfs;
        ResultWriterCollections resultWriterCollections = new ResultWriterCollections(outputPathStr);

        ActeDeces expected = ActeDeces.builder().nomPrenom("BENAMEUR*HABIB/")
                .build();
        Dataset<ActeDeces> actesDeces = sparkSession.createDataset(Collections.singletonList(expected), Encoders.bean(ActeDeces.class));
        resultWriterCollections.accept(actesDeces);
        StringWriter writer = new StringWriter();
        try {
            hdfs = FileSystem.get(new Configuration());
            FSDataInputStream fsDataInputStream = hdfs.open(new Path(outputPathStr));

            IOUtils.copy(fsDataInputStream, writer);
            String fileContent = writer.toString();

        }
        catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
