package fr.iiil.bigdata.reminder.functions.writer;

import fr.iiil.bigdata.reminder.bean.ActeDeces;
import fr.iiil.bigdata.reminder.bean.EnumOutputFormat;
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
    private  final EnumOutputFormat enumOutputFormat;

    public ResultWriterCollections(String outputPathStr) {
        this(outputPathStr,EnumOutputFormat.PARQUET);
    }
    @Override
    public void accept(Dataset<ActeDeces> acteDecesDataset) {
        AtomicInteger counter = new AtomicInteger(1);
        acteDecesDataset.show();
        switch (enumOutputFormat) {
            case PARQUET:
                acteDecesDataset.coalesce(2).write().mode(SaveMode.Overwrite).parquet(outputPathStr);
                break;
            case CSV:
                acteDecesDataset.coalesce(2).write().mode(SaveMode.Overwrite).option("header",true).option("delimiter", ",").csv(outputPathStr);
                break;
            default:
                acteDecesDataset.coalesce(2).write().mode(SaveMode.Overwrite).text(outputPathStr);
        }
    }
}
