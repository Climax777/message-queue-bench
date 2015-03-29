/**
 * Created by climax on 2015/03/27.
 */

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.rabbitmq.client.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

public class BenchRabbitMQConsumer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private final MetricRegistry registry = new MetricRegistry();
    private final Slf4jReporter reporter = Slf4jReporter.forRegistry(registry)
            .outputTo(LOGGER)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    private final Meter consumerMeter = registry.meter(name(BenchRabbitMQConsumer.class, "messages", "size"));
    private final String EXCHANGE_NAME = "rabbit_bench";
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
    private boolean ack;
    private AtomicInteger numberOfMessages = new AtomicInteger(0);
    private ConnectionFactory factory;

    public BenchRabbitMQConsumer(String[] args) {
        options.addOption("h", "help", false, "prints this message");
        options.addOption("n", "numthreads", true, "add this option to enable multiple consumers");
        options.addOption("i", "host", true, "host to connect to");
        options.addOption("a", "ack", false, "ack");
        options.addOption("p", "port", true, "port to connect to");
        options.addOption("b", "basetopic", true, "base topic to use: base.*");
        options.addOption("t", "topic", true, "topic to use for filter: base.topic. Defaults to random topic");
        options.addOption("w", "wildcard", false, "use wildcard (mutually exclusive with 't'");
        options.addOption("d", "duration", true, "duration of listening for messages (first of d or m will close program)");
        options.addOption("m", "messages", true, "number of messages to consume (first of d or m will close program)");
        options.addOption("c", "channels", true, "number of channels to subscribe to (if topic is empty 'c' channels will be subscribed to)");
        parseCommandLine(args);
        factory = new ConnectionFactory();
        factory.setHost(hostname);
        factory.setPort(port);
        factory.setShutdownTimeout(0);
    }

    public static void main(String[] args) throws Exception {
        BenchRabbitMQConsumer consumer = new BenchRabbitMQConsumer(args);
        consumer.run();
    }

    private void parseCommandLine(String[] args) {
        CommandLineParser commandLineParser = new BasicParser();
        try {
            CommandLine commandLine = commandLineParser.parse(options, args);
            if (commandLine.hasOption('h')) {
                printHelp();
            }

            hostname = commandLine.getOptionValue('i', "localhost");
            port = Integer.parseInt(commandLine.getOptionValue('p', "5672"));
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
    }

    public final class BenchRunner implements Runnable {
        @Override
        public void run() {
            LOGGER.info("{} running...", Thread.currentThread().getName());
            try {
                Connection connection = factory.newConnection();
                final Channel channel = connection.createChannel();
                channel.exchangeDeclare(EXCHANGE_NAME, "topic");
                String queueName = channel.queueDeclare().getQueue();
                if (channels == 0) {
                    String finalTopic = baseTopic;
                    if (topic.isEmpty()) {
                        if (wildcard) {
                            finalTopic += ".*";
                        } else {
                            finalTopic += "." + RandomStringUtils.randomAlphanumeric(5);
                        }
                    } else {
                        finalTopic += "." + topic;
                    }
                    channel.queueBind(queueName, EXCHANGE_NAME, finalTopic);
                } else {
                    for (int i = 0; i < channels; ++i) {
                        String finalTopic = baseTopic;
                        finalTopic += "." + RandomStringUtils.randomAlphanumeric(5);
                        channel.queueBind(queueName, EXCHANGE_NAME, finalTopic);
                    }
                }
                channel.basicConsume(queueName, !ack, Thread.currentThread().getName(), new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        consumerMeter.mark(1);
                        if (ack) {
                            channel.basicAck(envelope.getDeliveryTag(), false);
                        }
                        if (Thread.currentThread().isInterrupted() || (numberOfMessages.incrementAndGet() >= messages && messages != 0)) {
                            channel.basicCancel(consumerTag);
                        }
                    }
                });

                while (!Thread.currentThread().isInterrupted() && (numberOfMessages.incrementAndGet() < messages || messages == 0)) {
                    Thread.sleep(10);
                }
                channel.basicCancel(Thread.currentThread().getName());
                channel.close();
                connection.close();
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
