package fr.iiil.bigdata.reminder.functions.mapper;

import fr.iiil.bigdata.reminder.bean.ActeDeces;
import fr.iiil.bigdata.reminder.functions.reader.TextFileReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
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
