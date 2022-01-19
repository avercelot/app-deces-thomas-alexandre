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
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class ResultWriterHbaseTest {
    private SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("ResultWriterTest").getOrCreate();
    private final String sampleLine = "BENAMEUR*HABIB/                                                                 11941020892352DEPARTEMENT D'ORAN                                          2021122401004262";
    Config config = ConfigFactory.load();
    String outputPathStr = config.getString("3il.path.output");
    private static final Config catalogConfig = ConfigFactory.load("catalog.conf");

    @Test
    public void testWriterOneLine() throws IOException {
        ResultWriterHbase resultWriterHbase = new ResultWriterHbase(outputPathStr);

        String catalog = new ObjectMapper().writeValueAsString(catalogConfig);
        ActeDeces expected =  ActeDeces.builder()
                .nomPrenom("BENAMEUR*HABIB/")
                .sexe(1)
                .dateNaissance(19410208)
                .codeLieuNaissance("92352")
                .commune("DEPARTEMENT D'ORAN")
                .pays("")
                .dateDeces(20211224)
                .codeLieuDeces("01004")
                .numActe("262")
                .build();
        Dataset<ActeDeces> actesDeces = sparkSession.createDataset(Collections.singletonList(expected), Encoders.bean(ActeDeces.class));
        resultWriterHbase.accept(actesDeces);

        Dataset<Row> tmpds = sparkSession.read()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .load();
        Dataset<ActeDeces> actual = tmpds.as(Encoders.bean(ActeDeces.class));

        assertThat(actual.collectAsList()).containsExactlyInAnyOrder(expected);
    }
}
