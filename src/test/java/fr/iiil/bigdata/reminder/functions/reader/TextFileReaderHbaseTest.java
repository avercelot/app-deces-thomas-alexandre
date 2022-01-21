package fr.iiil.bigdata.reminder.functions.reader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.iiil.bigdata.reminder.bean.Contact;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.junit.BeforeClass;
import org.junit.Test;

import java.awt.print.Book;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.builder;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TextFileReaderHbaseTest {
    final SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("HBaseWriter-Test").getOrCreate();

    private static final String catalogFileName = "src/test/resources/catalog.conf";
    private static String catalog;

    @BeforeClass
    public static void setUp() throws IOException {
        catalog = Files.lines(Paths.get(catalogFileName)).map(String::trim).collect(Collectors.joining(""));
    }

    @Test
    public void testHbaseReader() throws IOException {
        List<Contact> expected = Arrays.asList(
                Contact.builder().raison("client|edf").word("iil").build()
        );
        TextFileReaderHbase contactReader = new TextFileReaderHbase(sparkSession, catalog);

        Dataset<Contact> actual = contactReader.get()
                .as(Encoders.bean(Contact.class));

        log.info("showing actual datafrom hbasereader ...");
        actual.printSchema();
        actual.show(false);

        assertThat(actual.collectAsList()).containsExactlyInAnyOrder(
                expected.toArray(new Contact[0])
        );
    }
}
