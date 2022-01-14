package fr.iiil.bigdata.actedeces.functions.reader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class TextFileReaderTest {
    Config config = ConfigFactory.load();
    String inputPathStr = config.getString("3il.path.input");
    @Test
    public void TestFirstLineReader() {

        TextFileReader reader = new TextFileReader(inputPathStr);
        Dataset<String> lines = reader.get();

        assertThat(lines.first().contains("BENAMEUR"));
    }
}
