package fr.iiil.bigdata.reminder.services;

import fr.iiil.bigdata.reminder.functions.MapUtils;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class WordCounter implements Runnable {

    @Getter
    private final AtomicInteger runCounter = new AtomicInteger(0);

    private final String inputPathStr;
    private final String outputPathStr;

    @Override
    public void run() {
        log.info("runCounter={}", runCounter.incrementAndGet());
        AtomicInteger counter = new AtomicInteger(1);
        Path outputPath = Paths.get(outputPathStr);
        try {
            Files.createDirectories(outputPath);

            List<String> lines = Files.lines(Paths.get(inputPathStr), Charset.defaultCharset()).collect(Collectors.toList());

            Stream<String> sequentialLines = lines.stream();
            sequentialLines.forEach(l -> log.info("line #{}, value={}", counter.getAndIncrement(), l));
            Files.write(Paths.get(outputPathStr +"/sequential.txt"), String.valueOf(counter.get()).getBytes());

            counter.set(1);

            Stream<String> parallelLines = lines.stream().parallel();
            parallelLines.forEach(l -> log.info("line #{}, value={}", counter.getAndIncrement(), l));
            Files.write(Paths.get(outputPathStr+"/parallel.txt"), String.valueOf(counter.get()).getBytes());

            List<String> words = lines.stream()
                    .flatMap(line -> Arrays.stream(line.split("\\W")))
                    .collect(Collectors.toList());

            Function<List<String>, Map<String, Integer>> wordCountFunction = MapUtils.genericCountFunction();
            Consumer<Map<String, Integer>> mapPrinter = MapUtils.mapPrinter();


            Map<String, Integer> wordCount = wordCountFunction.apply(words);
            mapPrinter.accept(wordCount);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
