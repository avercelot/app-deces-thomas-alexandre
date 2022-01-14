package fr.iiil.bigdata.actedeces.functions.mapper;

import fr.iiil.bigdata.actedeces.bean.ActeDeces;
import org.apache.spark.sql.*;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class ConvertLinesToActeDecesTest {
    private final String sampleLine = "BENAMEUR*HABIB/                                                                 11941020892352DEPARTEMENT D'ORAN                                          2021122401004262";

    @Test
    public void convertLinesToActeDecesTest(){
        ConvertLinesToActeDeces convertLinesToActeDeces = new ConvertLinesToActeDeces();

        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("ConvertLinesToActeDecesTest").getOrCreate();
        Dataset<String> lines = sparkSession.createDataset(Collections.singletonList(sampleLine), Encoders.STRING());
        Dataset<ActeDeces> actesDeces = convertLinesToActeDeces.apply(lines);

        ActeDeces expected = ActeDeces.builder()
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

        ActeDeces actual = actesDeces.first();
        assertThat(actual).isEqualTo(expected);
    }
}
