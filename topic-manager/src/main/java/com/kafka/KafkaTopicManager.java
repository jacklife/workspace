package com.kafka;

import org.apache.commons.cli.*;

public class KafkaTopicManager {
    private static final int KAFKA_TOPIC_PARTITION_COUNT = 1;
    private static final int KAFKA_TOPIC_REPLICATION_COUNT = 1;

    private static boolean isTopicNotExists = true;

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("h", "help", false, "Print this usage information");
        options.addOption("z", "zookeeper", true, "zookeeper information of kafka");
        options.addOption("t", "topic", true, "kafka topic to be created");
        // Parse the program arguments
        CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println("use -h for help");
            System.exit(0);
            return;
        }

        if (commandLine.hasOption('h') || commandLine.getOptions().length == 0) {
            System.out.println("usage : java -jar kafka-manager.jar -z [zookeeper] -t [topic]");
            System.out.println("example usage :");
            System.out.println("java -jar kafka-topic-manager.jar -z \"127.0.0.1:2181\" -t \"test\"");
            System.exit(0);
        }

        if (commandLine.hasOption('z') && commandLine.hasOption('t')) {
            String kafkaZk = commandLine.getOptionValue('z');
            String topic = commandLine.getOptionValue('t');

            System.out.printf("\nkafka zk information : %s", kafkaZk);

            System.out.printf("\ntopic : %s", topic);


            while (true) {
                try {
                    if (isTopicNotExists) {
                        isTopicNotExists = new TopicCreater(kafkaZk).isTopicNotExists(topic);
                        if (isTopicNotExists) {
                            new TopicCreater(kafkaZk).createTopic(topic, KAFKA_TOPIC_PARTITION_COUNT, KAFKA_TOPIC_REPLICATION_COUNT);
                            continue;
                        } else {
                            System.out.printf("\ntopic create successfully.\n", topic);
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
