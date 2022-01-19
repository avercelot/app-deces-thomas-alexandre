package fr.iiil.bigdata.reminder.functions.func;

import fr.iiil.bigdata.reminder.bean.ActeDeces;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DecesParJourTest {
    private final String sampleLine = "BENAMEUR*HABIB/                                                                 11941020892352DEPARTEMENT D'ORAN                                          2021122401004262";

    @Test
    public void NbDecesParJour() {
        DecesParJour decesParJour = new DecesParJour();

        ActeDeces ad = ActeDeces.builder()
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

        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("DecesParJourTest").getOrCreate();
        Dataset<ActeDeces> NbDecesJour = sparkSession.createDataset(Collections.singletonList(ad), Encoders.bean(ActeDeces.class));
        Dataset<Row> actual = decesParJour.apply(NbDecesJour);
        List<Row> data = Arrays.asList(
                RowFactory.create(20211224, 1)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("dateDeces", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("nb", DataTypes.IntegerType, false, Metadata.empty())
        });
        Dataset<Row> expected = sparkSession.createDataFrame(data, schema);
        assertThat(actual.collectAsList()).isEqualTo(expected.collectAsList());
        //actual.except(expected).isEmpty();
    }
}
