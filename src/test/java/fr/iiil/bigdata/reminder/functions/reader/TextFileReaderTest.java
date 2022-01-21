package fr.iiil.bigdata.reminder.functions.reader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.iiil.bigdata.reminder.functions.reader.TextFileReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TextFileReaderTest {

    Config config = ConfigFactory.load();
    String inputPathStr = config.getString("3il.path.input");
    @Test
    public void TestFirstLineReader() throws IOException {

        TextFileReader reader = new TextFileReader(inputPathStr);
        Dataset<String> lines = reader.get();

        assertThat(lines.first().contains("BENAMEUR"));
    }

    @Test
    public void FileExist() throws IOException{
        TextFileReader reader = new TextFileReader("file:///Users/thoma/Documents/test.txt");
        Dataset<String> lines = reader.get();
        assertThat(lines.toJavaRDD().isEmpty()).isTrue();
    }
}
