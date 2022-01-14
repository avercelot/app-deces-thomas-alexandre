package fr.iiil.bigdata.actedeces.functions.func;

import fr.iiil.bigdata.actedeces.bean.ActeDeces;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;

import static org.apache.spark.sql.functions.count;

public class DecesParJour implements Function<Dataset<ActeDeces>, Dataset<Row>> {
    @Override
    public Dataset<Row> apply(Dataset<ActeDeces> statsDs) {
        return  statsDs.groupBy("dateDeces").agg(count("nomPrenom").as("nb"));
    }
}
