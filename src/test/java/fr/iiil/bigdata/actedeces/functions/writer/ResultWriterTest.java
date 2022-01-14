package fr.iiil.bigdata.actedeces.functions.writer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.iiil.bigdata.actedeces.bean.ActeDeces;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

public class ResultWriterTest {
    private SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("ResultWriterTest").getOrCreate();
    private final String sampleLine = "BENAMEUR*HABIB/                                                                 11941020892352DEPARTEMENT D'ORAN                                          2021122401004262";
    Config config = ConfigFactory.load();
    String outputPathStr = config.getString("3il.path.output");

//    @Before
//    public void setup() {
//        cleanup();
//    }

    @Test
    public void testWriterOneLine() {
        ResultWriterCollections resultWriterCollections = new ResultWriterCollections(outputPathStr);

        ActeDeces expected = ActeDeces.builder().nomPrenom("BENAMEUR*HABIB/")
                .build();
        Dataset<ActeDeces> actesDeces = sparkSession.createDataset(Collections.singletonList(expected), Encoders.bean(ActeDeces.class));
        resultWriterCollections.accept(actesDeces);

//        Dataset<Row> tmpds = sparkSession.read().option("header", true).csv(outputPathStr);
        Dataset<Row> tmpds = sparkSession.read().parquet(outputPathStr);
        tmpds.printSchema();
        Dataset<ActeDeces> actual = tmpds.as(Encoders.bean(ActeDeces.class));

        assertThat(actual.collectAsList()).containsExactlyInAnyOrder(expected);
    }

//    @After
//    public void teardown() {
//        cleanup();
//    }
//
//    public void cleanup() {
//        try {
//            Path outputPath = Paths.get(outputPathStr);
//            Files.list(outputPath).forEach(
//                    path -> {
//                        try{Files.deleteIfExists(path);}catch (IOException ioException) {
//                            log.warn("could not delete path={} due to ...", path, ioException);
//                        }
//                    }
//            );
//        } catch (IOException ioException) {
//                ioException.printStackTrace();
//        }
//    }
}
