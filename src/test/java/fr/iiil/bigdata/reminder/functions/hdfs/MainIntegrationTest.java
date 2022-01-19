package fr.iiil.bigdata.reminder.functions.hdfs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;
import java.io.IOException;

public class MainIntegrationTest {
    Config config = ConfigFactory.load();
    Configuration hadoopConf = new Configuration();
    FileSystem hdfs = FileSystem.get(hadoopConf);
    Path inputPath = new Path(config.getString("3il.path.input"));
    Path outputPath = new Path(config.getString("3il.path.output"));

    public MainIntegrationTest() throws IOException {
    }

    @Test
    public void checkOutputDir() throws IOException {
        boolean actual = hdfs.exists(inputPath);
        assertThat(actual).isEqualTo(true);
    }
}
