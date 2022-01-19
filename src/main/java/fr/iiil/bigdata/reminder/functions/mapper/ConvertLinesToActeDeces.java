package fr.iiil.bigdata.reminder.functions.mapper;

import fr.iiil.bigdata.reminder.bean.ActeDeces;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import java.util.Objects;
import java.util.function.Function;

public class ConvertLinesToActeDeces implements Function<Dataset<String>, Dataset<ActeDeces>> {
    private final LineToActeDeces lineToActeDeces = new LineToActeDeces();
    final MapFunction<String, ActeDeces> acteDecesMapFunction = lineToActeDeces::apply;
    @Override
    public Dataset<ActeDeces> apply(Dataset<String> lines) {

        return lines.map(acteDecesMapFunction, Encoders.bean(ActeDeces.class))
                .filter((FilterFunction<ActeDeces>) (Objects::nonNull));
    }
}
