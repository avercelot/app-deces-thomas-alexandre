package fr.iiil.bigdata.reminder.functions.reader;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.util.function.Supplier;

@Slf4j
@Builder
@RequiredArgsConstructor
public class TextFileReaderHbase implements Supplier<Dataset<String>> {
    private final SparkSession sparkSession;
    private final String catalog;
    @Override
    public Dataset<String> get() {
        log.info("reading from catalog={}",catalog);
        Dataset<Row> ds = sparkSession.emptyDataFrame();
        try {
            ds = sparkSession.read()
                    .option(HBaseTableCatalog.tableCatalog(), catalog)
                    .format("org.apache.spark.sql.execution.datasources.hbase")
                    .load();
        }
        catch (Exception e) {
            log.error("hbase table could not be read due to ...", e);
        }
        return null;
    }
}
