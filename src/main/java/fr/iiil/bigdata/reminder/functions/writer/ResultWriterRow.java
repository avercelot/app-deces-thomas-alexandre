package fr.iiil.bigdata.reminder.functions.writer;

import fr.iiil.bigdata.reminder.bean.ActeDeces;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
@Builder
@RequiredArgsConstructor
public class ResultWriterRow implements Consumer<Dataset<Row>> {
    private final String outputPathStr;

    @Override
    public void accept(Dataset<Row> acteDecesRow) {
        AtomicInteger counter = new AtomicInteger(1);
        acteDecesRow.show();

        acteDecesRow.coalesce(2).write().mode(SaveMode.Overwrite).parquet(outputPathStr);

    }
}
