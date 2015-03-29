/**
 * Created by climax on 2015/03/27.
 */

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import kafka.consumer.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

/*import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;*/

public class BenchKafkaConsumer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private final MetricRegistry registry = new MetricRegistry();
    private final Slf4jReporter reporter = Slf4jReporter.forRegistry(registry)
            .outputTo(LOGGER)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    private final Meter consumerMeter = registry.meter(name(BenchKafkaConsumer.class, "messages", "size"));
    private final String EXCHANGE_NAME = "rabbit_bench";
    private Options options = new Options();
    private HelpFormatter formatter = new HelpFormatter();
    private String hostname;
    private int port;
    private int threads;
    private String baseTopic;
    private int duration;
    private int messages;
    private boolean ack;
    private String groupID;
    private AtomicInteger numberOfMessages = new AtomicInteger(0);

    public BenchKafkaConsumer(String[] args) {
        options.addOption("h", "help", false, "prints this message");
        options.addOption("n", "numthreads", true, "add this option to enable multiple consumers");
        options.addOption("i", "host", true, "host to connect to");
        options.addOption("a", "ack", false, "ack");
        options.addOption("p", "port", true, "port to connect to");
        options.addOption("b", "basetopic", true, "base topic to use: base.*");
        options.addOption("d", "duration", true, "duration of listening for messages (first of d or m will close program)");
        options.addOption("m", "messages", true, "number of messages to consume (first of d or m will close program)");
        options.addOption("g", "groupid", true, "group id. Leave blank for random (each consumer will receive all partitions)");
        parseCommandLine(args);
    }

    public static void main(String[] args) throws Exception {
        BenchKafkaConsumer consumer = new BenchKafkaConsumer(args);
        consumer.run();
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
            port = Integer.parseInt(commandLine.getOptionValue('p', "2181"));
            threads = Integer.parseInt(commandLine.getOptionValue('n', String.valueOf(Runtime.getRuntime().availableProcessors())));
            if (threads <= 0) {
                throw new ParseException("threads must be >= 1");
            }
            baseTopic = commandLine.getOptionValue('b', "bench");
            duration = Integer.parseInt(commandLine.getOptionValue('d', Integer.toString(60 * 5)));
            if (duration < 0) {
                throw new ParseException("duration must be >= 0");
            }
            groupID = commandLine.getOptionValue('g', "");
            messages = Integer.parseInt(commandLine.getOptionValue('m', "0"));
            if (messages < 0) {
                throw new ParseException("messages must be >= 0");
            }
            if (messages == 0 && duration == 0) {
                throw new ParseException("messages or duration must be > 0");
            }
            ack = commandLine.hasOption('a');
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
            threadGroup[i] = new Thread(new BenchRunner(), "Consumer-" + Integer.toString(i));
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
            try {
                timeKiller.join();
            } catch (InterruptedException e) {
            }
        }
        LOGGER.info("Bench done.");
        reporter.report();
        System.exit(0);
    }

    public final class BenchRunner implements Runnable {
        @Override
        public void run() {
            LOGGER.info("{} running...", Thread.currentThread().getName());
            try {

                Properties kafkaProperties = new Properties();
                kafkaProperties.put("zookeeper.connect", hostname + ":" + port);
                kafkaProperties.put("auto.commit.enable", (ack) ? "true" : "false");
                kafkaProperties.put("auto.offset.reset", "largest");
                if (groupID.isEmpty())
                    kafkaProperties.put("group.id", RandomStringUtils.randomAlphanumeric(5));
                ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProperties);
                kafka.javaapi.consumer.ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
                //ZkUtils.maybeDeletePath(kafkaProperties.getProperty("zookeeper.connect"), "/consumers/" + kafkaProperties.getProperty("group.id"));
                Map<String, Integer> topicMap = new HashMap<>(1);
                topicMap.put(baseTopic, (int) Thread.currentThread().getId());
                KafkaStream<byte[], byte[]> stream = consumer.createMessageStreamsByFilter(new Whitelist(baseTopic), 1).get(0);
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                while (!Thread.currentThread().isInterrupted() && (numberOfMessages.get() < messages || messages == 0)) {
                    while (it.hasNext()) {
                        it.next();
                        consumerMeter.mark(1);
                        numberOfMessages.incrementAndGet();
                        if (ack)
                            consumer.commitOffsets();
                    }
                }

                consumer.commitOffsets();
                consumer.shutdown();
               /* while (!Thread.currentThread().isInterrupted() && (numberOfMessages.incrementAndGet() < messages || messages == 0)) {
                    Map<String, ConsumerRecords> records = consumer.poll(100);
                    consumerMeter.mark(records.size());

                    if(ack) {
                        for (ConsumerRecords consumerRecords : records.values()) {

                            consumer.commit(consumerRecords.)
                        }
                    }


                }*/
            } catch (Exception e) {
                LOGGER.warn("Exception in subscribe: {}", e.toString());
            }
            if (Thread.currentThread().isInterrupted()) {
                LOGGER.info("{} interrupted...", Thread.currentThread().getName());
            } else {
                LOGGER.info("{} done...", Thread.currentThread().getName());
            }
        }
    }
}
