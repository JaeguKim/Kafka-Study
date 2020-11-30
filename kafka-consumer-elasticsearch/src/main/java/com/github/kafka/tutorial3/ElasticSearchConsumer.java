package com.github.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {
        String hostname = "localhost";

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostname, 9200, "http"),
                        new HttpHost(hostname, 9201, "http")));
//
//        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
//            @Override
//            public void onResponse(IndexResponse indexResponse) {
//                String index = indexResponse.getIndex();
//                String type = indexResponse.getType();
//                String id = indexResponse.getId();
//                long version = indexResponse.getVersion();
//                logger.info(String.format("result : %s, index : %s, type : %s, id : %s"), indexResponse.getResult(), index, type, id);
//                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
//                    logger.info("document is created!");
//                }
//
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                logger.error(e.toString());
//            }
//        };

        return client;
    }

    public static KafkaConsumer<String,String> createConsumer(String topic) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
        KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");

        // poll for new data
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0
            for (ConsumerRecord<String,String> record : records){
                IndexRequest request = new IndexRequest("twitter", "tweets").source(record.value(), XContentType.JSON);
                IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info(id);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //client.close();
    }
}
