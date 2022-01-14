package fr.iiil.bigdata.actedeces.functions.writer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

public class ResultWriterRowTest {
    private SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("ResultWriterTest").getOrCreate();
    private final String sampleLine = "BENAMEUR*HABIB/                                                                 11941020892352DEPARTEMENT D'ORAN                                          2021122401004262";
    Config config = ConfigFactory.load();
    String outputPathStr = config.getString("3il.path.output");

    @Test
    public void testWriterRowOneLine() {
        ResultWriterRow resultWriterRow = new ResultWriterRow(outputPathStr);
        List<Row> data = Arrays.asList(
                RowFactory.create(20211224, 1)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("dateDeces", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("nb", DataTypes.IntegerType, false, Metadata.empty())
        });
        Dataset<Row> expected = sparkSession.createDataFrame(data, schema);
        resultWriterRow.accept(expected);

        Dataset<Row> actual = sparkSession.read().parquet(outputPathStr);

        assertThat(actual.collectAsList()).isEqualTo(expected.collectAsList());
    }
}
