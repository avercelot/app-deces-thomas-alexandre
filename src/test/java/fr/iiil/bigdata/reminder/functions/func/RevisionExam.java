package fr.iiil.bigdata.reminder.functions.func;

import fr.iiil.bigdata.reminder.bean.ActeDeces;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.util.Collections;
import java.util.function.Supplier;

import static org.apache.spark.sql.functions.count;
import static org.assertj.core.api.Java6Assertions.assertThat;

@Slf4j
public class RevisionExam {
    Supplier<Dataset<ActeDeces>> acteDecesSupplier = () -> {
        final SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("ResultWriterTest").getOrCreate();
        ActeDeces expected =  ActeDeces.builder()
                .nomPrenom("BENAMEUR*HABIB/")
                .sexe(1)
//                .dateNaissance(19410208)
//                .codeLieuNaissance("92352")
//                .commune("DEPARTEMENT D'ORAN")
//                .pays("")
//                .dateDeces(20211224)
//                .codeLieuDeces("01004")
//                .numActe("262")
                .build();
        Dataset<ActeDeces> actesDeces = sparkSession.createDataset(Collections.singletonList(expected), Encoders.bean(ActeDeces.class));

        return actesDeces;
    };
    @Test
    public void testSelect() {
        Dataset<ActeDeces> acteDecesDataset = acteDecesSupplier.get();
        Dataset<Row> actual = acteDecesDataset.select(acteDecesDataset.col("nomPrenom"), acteDecesDataset.col("sexe"));
        assertThat(actual.columns()).contains("nomPrenom");
    }

    @Test
    public void testDrop() {
        Dataset<ActeDeces> acteDecesDataset = acteDecesSupplier.get();
        Dataset<Row> actual = acteDecesDataset.drop("nomPrenom");
        assertThat(actual.columns()).doesNotContain("nomPrenom");
    }

    @Test
    public void testWhere() {
        Dataset<ActeDeces> acteDecesDataset = acteDecesSupplier.get();
        Dataset<ActeDeces> actual = acteDecesDataset.where(acteDecesDataset.col("sexe").equalTo(1));
        Assertions.assertThat(actual.collectAsList().contains("BENAMEUR"));
    }

//    @Test
//    public void testJoin() {
//        Dataset<ActeDeces> acteDecesDataset1 = acteDecesSupplier.get();
//        Dataset<Row> select = acteDecesDataset1.select(acteDecesDataset1.col("nomPrenom").as("np"), acteDecesDataset1.col("sexe").as("s"));
//        Dataset<ActeDeces> acteDecesDataset2 = acteDecesSupplier.get();
//        Dataset<Row> rowDataset = select.join(acteDecesDataset2);
//        Dataset<Row> actual = rowDataset.groupBy("nomPrenom").count().where(rowDataset.col("sexe").equalTo(1));
//        log.info("test={}",actual);
//        assertThat(actual.limit(2)).isEqualTo(2);
//    }
}
