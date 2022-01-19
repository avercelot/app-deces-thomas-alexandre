package fr.iiil.bigdata.reminder;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.iiil.bigdata.reminder.bean.ActeDeces;
import fr.iiil.bigdata.reminder.functions.func.DecesParJour;
import fr.iiil.bigdata.reminder.functions.mapper.ConvertLinesToActeDeces;
import fr.iiil.bigdata.reminder.functions.reader.TextFileReader;
import fr.iiil.bigdata.reminder.functions.writer.ResultWriterCollections;
import fr.iiil.bigdata.reminder.functions.writer.ResultWriterRow;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * Hello world!
 *
 */

public class BigDataActeDecesApp {
    public static void main( String[] args ) throws IOException {
        Config config = ConfigFactory.load();
        String inputPathStr = config.getString("3il.path.input");
        String outputPathStr = config.getString("3il.path.output");

        System.out.println( "Hello Java Collections!" );

        TextFileReader reader = new TextFileReader(inputPathStr);
        DecesParJour decesParJour = new DecesParJour();
        ResultWriterCollections resultWriterCollections = new ResultWriterCollections(outputPathStr);
        ResultWriterRow resultWriterRow = new ResultWriterRow(outputPathStr);
        ConvertLinesToActeDeces convertLinesToActeDeces = new ConvertLinesToActeDeces();

        Dataset<String> lines = reader.get();

        Dataset<ActeDeces> actesDeces = convertLinesToActeDeces.apply(lines);
        Dataset<Row> acteDecesParJour = decesParJour.apply(actesDeces);

        resultWriterRow.accept(acteDecesParJour);
        resultWriterCollections.accept(actesDeces);
    }
}
