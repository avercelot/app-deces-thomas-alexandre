package fr.iiil.bigdata.reminder;

import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class BigDataActeDecesAppTest {
    String outputFolderPathStr = ConfigFactory.load().getString("3il.path.output");
    String inputPathFile = ConfigFactory.load().getString("3il.path.input");
    FileSystem hdfs;
    @Test
    public void shouldReadFile() throws IOException {

        BigDataActeDecesApp.main(new String[0]);
        assertThat(
                hdfs.listStatus(
                        new Path(outputFolderPathStr)
                ).length
        ).isGreaterThan(0);
//        assertThat(
//                Files.list(
//                        Paths.get(ConfigFactory.load().getString("3il.path.output").replace("file://", ""))
//                ).count()
//        ).isGreaterThan(0L);
    }
    @Before
    public void setup() throws IOException {
        Configuration hadoopConf = new Configuration();
        String localInputFile = "src/test/resources/deces-2021-m12.txt";
        FileSystem fs = FileSystem.getLocal(hadoopConf);
        hdfs = FileSystem.get(hadoopConf);
        Path inputFilePath = new Path(inputPathFile);
        Path inputFolderPath = inputFilePath.getParent();
        hdfs.mkdirs(inputFolderPath);
        hdfs.copyFromLocalFile(false, true, new Path(localInputFile), inputFolderPath);
        cleanup();
    }
    @After
    public void tearDown() {
        cleanup();
    }

    public void cleanup() {
        try {
            hdfs.delete(new Path(outputFolderPathStr), true);
        }
        catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
