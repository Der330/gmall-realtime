package com.atguigu.realtime.common.util;

import com.atguigu.realtime.common.constant.Contant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;

public class FlinkKafkaUtil {
    public static KafkaSink getKafkaSink(String topic) {
        KafkaSink<String> kafkaSink=KafkaSink.<String>builder()
                .setBootstrapServers(Contant.KAFKA_BROKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
        return kafkaSink;
    }


    public static KafkaSource<String> getKafkaSource(String topic) {
        KafkaSource<String> kafkaSource=KafkaSource.<String>builder()
                .setBootstrapServers(Contant.KAFKA_BROKERS)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if (bytes != null) {
                                    return new String(bytes);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .setGroupId(topic)
                .setTopics(topic)
                .build();



        return kafkaSource;
    }
}
