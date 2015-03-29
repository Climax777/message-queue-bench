/**
 * Created by climax on 2015/03/27.
 */

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class BenchKafkaProducer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private final MetricRegistry registry = new MetricRegistry();
    private final Slf4jReporter reporter = Slf4jReporter.forRegistry(registry)
            .outputTo(LOGGER)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    private final Meter producerMeter = registry.meter(name(BenchKafkaProducer.class, "messages", "size"));
    Properties kafkaProperties = new Properties();
    private Options options = new Options();
    private HelpFormatter formatter = new HelpFormatter();
    private String hostname;
    private int port;
    private int threads;
    private String topic;
    private String baseTopic;
    private int size;
    private int duration;
    private int delay;
    private int pipeline;
    private int messages;
    private int acks;
    private String compression;
    private boolean durable;

    public BenchKafkaProducer(String[] args) {
        options.addOption("h", "help", false, "prints this message");
        options.addOption("n", "numthreads", true, "add this option to enable multiple producers");
        options.addOption("i", "host", true, "host to connect to");
        options.addOption("P", "pipeline", true, "add this option to enable pipelining, with an optional parameter indicating the batch size");
// TODO        options.addOption("C", "confirms", false, "confirms");
        options.addOption("r", "durable", false, "durable");
        options.addOption("c", "compression", true, "one of none, snappy or gzip");
        options.addOption("p", "port", true, "port to connect to");
        options.addOption("a", "acks", true, "number of servers to ack (-1 = all, 0 = none <default>, ...)");
        options.addOption("b", "basetopic", true, "base topic to use: base.*. Note that the topic must be created in kafka beforehand");
        options.addOption("t", "topic", true, "topic to use: base.topic. Defaults to random topic");
        options.addOption("s", "size", true, "size of messages");
        options.addOption("d", "duration", true, "duration of messages (first of d or m will close program)");
        options.addOption("D", "delay", true, "delay in milliseconds between messages (or pipelines when -P is set)");
        options.addOption("m", "messages", true, "number of messages (first of d or m will close program)");
        parseCommandLine(args);
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostname + ":" + port);
        switch (acks) {
            case -1:
                kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "all");
                break;
            default:
                kafkaProperties.put(ProducerConfig.ACKS_CONFIG, Integer.toString(acks));
                break;
        }
        kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        kafkaProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, (pipeline == 0 ? "1" : Integer.toString(pipeline)));
    }

    public static void main(String[] args) throws Exception {
        BenchKafkaProducer producer = new BenchKafkaProducer(args);
        producer.run();
        System.exit(0);
    }

    private void parseCommandLine(String[] args) {
        CommandLineParser commandLineParser = new BasicParser();
        try {
            CommandLine commandLine = commandLineParser.parse(options, args);
            if (commandLine.hasOption('h')) {
                printHelp();
            }

            hostname = commandLine.getOptionValue('i', "localhost");
            port = Integer.parseInt(commandLine.getOptionValue('p', "9092"));
            threads = Integer.parseInt(commandLine.getOptionValue('n', String.valueOf(Runtime.getRuntime().availableProcessors())));
            if (threads <= 0) {
                throw new ParseException("threads must be >= 1");
            }
            baseTopic = commandLine.getOptionValue('b', "bench");
            topic = commandLine.getOptionValue('t', "");
            size = Integer.parseInt(commandLine.getOptionValue('s', "512"));
            if (size < 0) {
                throw new ParseException("size must be >= 0");
            }
            duration = Integer.parseInt(commandLine.getOptionValue('d', "0"));
            if (duration < 0) {
                throw new ParseException("duration must be >= 0");
            }
            pipeline = Integer.parseInt(commandLine.getOptionValue('P', "100"));
            if (pipeline < 0) {
                throw new ParseException("pipeline must be >= 1");
            }
            delay = Integer.parseInt(commandLine.getOptionValue('D', "0"));
            if (delay < 0) {
                throw new ParseException("delay must be >= 0");
            }
            messages = Integer.parseInt(commandLine.getOptionValue('m', "1000000"));
            if (messages < 0) {
                throw new ParseException("messages must be >= 0");
            }
            if (messages == 0 && duration == 0) {
                throw new ParseException("messages or duration must be > 0");
            }
            durable = commandLine.hasOption('r');
            acks = Integer.parseInt(commandLine.getOptionValue('a', "0"));
            if (acks < -1) {
                throw new ParseException("acks must be >= -1");
            }
            compression = commandLine.getOptionValue('c', "snappy");
            if (compression != "none" && compression != "snappy" && compression != "gzip") {
                throw new ParseException("compression not a valid type");
            }
        } catch (ParseException e) {
            LOGGER.error("Parse error: " + e.getMessage());
            printHelp();
        }
    }

    public void printHelp() {
        formatter.printHelp("Help", options);
        System.exit(0);
    }

    @Override
    public void run() {
        final Thread[] threadGroup = new Thread[threads];
        reporter.start(5, TimeUnit.SECONDS);
        LOGGER.info("Starting {} threads", threads);
        for (int i = 0; i < threads; ++i) {
            threadGroup[i] = new Thread(new BenchRunner(), "Producer-" + Integer.toString(i));
            threadGroup[i].start();
        }
        Thread timeKiller = null;
        if (duration > 0) {
            timeKiller = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(duration * 1000);
                        LOGGER.info("Time expired interrupting workers");
                    } catch (InterruptedException e) {
                    } finally {
                        for (Thread thread : threadGroup) {
                            thread.interrupt();
                        }
                    }
                }
            }, "Time killer");
            timeKiller.start();
        }

        for (Thread thread : threadGroup) {
            try {
                thread.join();
            } catch (InterruptedException e) {
            }
        }
        if (timeKiller != null) {
            timeKiller.interrupt();
        }
        LOGGER.info("Bench done.");
        reporter.report();
        System.exit(0);
    }

    public final class BenchRunner implements Runnable {
        @Override
        public void run() {
            LOGGER.info("{} running...", Thread.currentThread().getName());
            String message = RandomStringUtils.randomAlphanumeric(size);
            try {
                final KafkaProducer producer = new KafkaProducer(kafkaProperties);
                for (int i = 0; ((messages == 0) || (i < messages)) && !Thread.currentThread().isInterrupted(); ++i) {

                    String key;
                    if (topic.isEmpty()) {
                        key = RandomStringUtils.randomAlphanumeric(5);
                    } else {
                        key = topic;
                    }
                    producer.send(new ProducerRecord(baseTopic, key.getBytes(), message.getBytes()), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            producerMeter.mark(1);
                        }
                    });
                    if (delay > 0) {
                        try {
                            Thread.currentThread().sleep(delay);
                        } catch (InterruptedException e) {
                        }
                    }
                }
                producer.close();
            } catch (Exception e) {
                LOGGER.warn("Exception in publish: {}", e.toString());
                e.printStackTrace();
            }
            if (Thread.currentThread().isInterrupted()) {
                LOGGER.info("{} interrupted...", Thread.currentThread().getName());
            } else {
                LOGGER.info("{} done...", Thread.currentThread().getName());
            }
        }
    }
}
