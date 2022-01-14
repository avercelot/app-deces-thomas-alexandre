package fr.iiil.bigdata.actedeces.functions.writer;

import fr.iiil.bigdata.actedeces.bean.ActeDeces;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
@Builder
@RequiredArgsConstructor
public class ResultWriterCollections implements Consumer<Dataset<ActeDeces>> {
    private final String outputPathStr;

    @Override
    public void accept(Dataset<ActeDeces> acteDecesDataset) {
        AtomicInteger counter = new AtomicInteger(1);
        acteDecesDataset.show();

        acteDecesDataset.coalesce(2).write().mode(SaveMode.Overwrite).parquet(outputPathStr);

    }
}
