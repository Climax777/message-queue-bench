/**
 * Created by climax on 2015/03/27.
 */

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

public class BenchRedisConsumer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private final MetricRegistry registry = new MetricRegistry();
    private final Slf4jReporter reporter = Slf4jReporter.forRegistry(registry)
            .outputTo(LOGGER)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    private final Meter consumerMeter = registry.meter(name(BenchRedisConsumer.class, "messages", "size"));
    private Options options = new Options();
    private HelpFormatter formatter = new HelpFormatter();
    private String hostname;
    private int port;
    private int threads;
    private String topic;
    private String baseTopic;
    private int duration;
    private int messages;
    private int channels;
    private boolean wildcard;
    private JedisPool pool;
    private AtomicInteger numberOfMessages = new AtomicInteger(0);
    private List<JedisPubSub> subbers = new ArrayList<>();

    public BenchRedisConsumer(String[] args) {
        options.addOption("h", "help", false, "prints this message");
        options.addOption("n", "numthreads", true, "add this option to enable multiple consumers");
        options.addOption("i", "host", true, "host to connect to");
        options.addOption("p", "port", true, "port to connect to");
        options.addOption("b", "basetopic", true, "base topic to use: base.*");
        options.addOption("t", "topic", true, "topic to use for filter: base.topic. Defaults to random topic");
        options.addOption("w", "wildcard", false, "use wildcard (mutually exclusive with 't'");
        options.addOption("d", "duration", true, "duration of listening for messages (first of d or m will close program)");
        options.addOption("m", "messages", true, "number of messages to consume (first of d or m will close program)");
        options.addOption("c", "channels", true, "number of channels to subscribe to (if topic is empty 'c' channels will be subscribed to)");
        parseCommandLine(args);
        pool = new JedisPool(new JedisPoolConfig(), hostname, port);
    }

    public static void main(String[] args) throws Exception {
        BenchRedisConsumer consumer = new BenchRedisConsumer(args);
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
            port = Integer.parseInt(commandLine.getOptionValue('p', "6379"));
            threads = Integer.parseInt(commandLine.getOptionValue('n', String.valueOf(Runtime.getRuntime().availableProcessors())));
            if (threads <= 0) {
                throw new ParseException("threads must be >= 1");
            }
            baseTopic = commandLine.getOptionValue('b', "bench");
            topic = commandLine.getOptionValue('t', "");
            duration = Integer.parseInt(commandLine.getOptionValue('d', Integer.toString(60 * 5)));
            if (duration < 0) {
                throw new ParseException("duration must be >= 0");
            }
            messages = Integer.parseInt(commandLine.getOptionValue('m', "0"));
            if (messages < 0) {
                throw new ParseException("messages must be >= 0");
            }
            if (messages == 0 && duration == 0) {
                throw new ParseException("messages or duration must be > 0");
            }
            channels = Integer.parseInt(commandLine.getOptionValue('c', "0"));
            if (channels < 0) {
                throw new ParseException("channels must be >= 0");
            }
            wildcard = commandLine.hasOption('w');
            if (wildcard && !topic.isEmpty()) {
                throw new ParseException("wildcard is mutually exclusive to topic");
            }
            if (wildcard && channels > 0) {
                throw new ParseException("wildcard is not valid when channels > 0");
            }
            if (!wildcard && topic.isEmpty() && channels == 0) {
                wildcard = true;
                LOGGER.info("wildcard forced on");
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
                        for (JedisPubSub subber : subbers) {
                            try {
                                subber.unsubscribe();
                            } catch (Exception e) {
                            }
                            try {
                                subber.punsubscribe();
                            } catch (Exception e) {
                            }
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
        pool.destroy();
        System.exit(0);

    }

    public final class BenchRunner implements Runnable {
        @Override
        public void run() {
            LOGGER.info("{} running...", Thread.currentThread().getName());
            JedisPubSub pubsub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    consumerMeter.mark(1);
                    if (Thread.currentThread().isInterrupted() || (numberOfMessages.incrementAndGet() >= messages && messages != 0)) {
                        unsubscribe();
                        punsubscribe();
                    }
                }

                @Override
                public void onPMessage(String pattern, String channel, String message) {
                    consumerMeter.mark(1);
                    if (Thread.currentThread().isInterrupted() || (numberOfMessages.incrementAndGet() >= messages && messages != 0)) {
                        unsubscribe();
                        punsubscribe();
                    }
                }
            };
            subbers.add(pubsub);
            try (Jedis jedis = pool.getResource()) {
                if (channels == 0) {
                    String finalTopic = baseTopic;
                    if (topic.isEmpty()) {
                        if (wildcard) {
                            finalTopic += ".*";
                            jedis.psubscribe(pubsub, finalTopic);
                        } else {
                            finalTopic += "." + RandomStringUtils.randomAlphanumeric(5);
                            jedis.subscribe(pubsub, finalTopic);
                        }
                    } else {
                        finalTopic += "." + topic;
                        jedis.subscribe(pubsub, finalTopic);
                    }
                } else {
                    String[] topics = new String[channels];
                    for (int i = 0; i < channels; ++i) {
                        String finalTopic = baseTopic;
                        finalTopic += "." + RandomStringUtils.randomAlphanumeric(5);
                        topics[i] = finalTopic;
                    }
                    jedis.subscribe(pubsub, topics);
                }
            } catch (Exception e) {
                LOGGER.warn("Exception in subscribe: {}", e.toString());
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
