package fr.iiil.bigdata.reminder.functions.writer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.iiil.bigdata.reminder.bean.ActeDeces;
import fr.iiil.bigdata.reminder.bean.EnumOutputFormat;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.function.Consumer;

@Slf4j
@Builder
@RequiredArgsConstructor
public class ResultWriterHbase implements Consumer<Dataset<ActeDeces>> {
    private final String outputPathStr;
    private  final EnumOutputFormat enumOutputFormat;
    private static final Config catalogConfig = ConfigFactory.load("catalog.conf");

    public ResultWriterHbase(String outputPathStr) throws IOException {
        this(outputPathStr,EnumOutputFormat.PARQUET);
    }
    @Override
    public void accept(Dataset<ActeDeces> acteDecesDataset) {
        try {
            String catalog = new ObjectMapper().writeValueAsString(catalogConfig);
            acteDecesDataset.write()
                    .option(HBaseTableCatalog.tableCatalog(), catalog)
                    .option(HBaseTableCatalog.newTable(), "9")
                    .format("org.apache.spark.sql.execution.datasources.hbase")
                    .save();
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
        catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }
}
