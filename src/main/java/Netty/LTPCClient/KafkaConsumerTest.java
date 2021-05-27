package Netty.LTPCClient;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2021/1/8 21:32
 */

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @program: BD_DAQ_InputSplit
 * @description:
 * @author: WuYe
 * @create: 2021-01-08 21:32
 **/
public class KafkaConsumerTest {
    private static long start=0;
    public static void main(String[] args) throws InterruptedException {
        int KafkaConsumerNum;
        if (args.length == 1) {
            KafkaConsumerNum = Integer.parseInt(args[0]);
        } else {
            throw new RuntimeException("argc error;Please input parameter: KafkaConsumerNum,messageLength");
        }
        ExecutorService executorService = Executors.newFixedThreadPool(KafkaConsumerNum);
        for (int i = 0; i < KafkaConsumerNum; i++) {
            executorService.submit(new Runnable() {
                private long tStart=0;
                @Override
                public void run() {
                    Properties kafkaProps = new Properties();
                    kafkaProps.put("bootstrap.servers", "hd08:9092,hd10:9092,hd13:9092,hd14:9092");
                    // 禁止自动提交偏移量
                    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                    // 偏移量提交的间隔时间
                    kafkaProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
                    kafkaProps.setProperty("auto.offset.reset", "earliest");
                    kafkaProps.setProperty("group.id", "detector_Maps");
                    kafkaProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
                    kafkaProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");
                    kafkaProps.setProperty("fetch.min.bytes", "24576");
                    kafkaProps.setProperty("max.poll.records", "500");
                    kafkaProps.setProperty("group.id", Thread.currentThread().getName());

                    KafkaConsumer<Integer, Bytes> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
                    String topic = "Maps1";
                    long dataSize = 0;
                    int dataNum = 0;
                    int offset = 0;
                    try {
                    kafkaConsumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {

                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            System.out.println("Assigned " + partitions);
                            for (TopicPartition tp : partitions) {
                                OffsetAndMetadata oam = kafkaConsumer.committed(tp);
                                if (oam != null) {
                                    System.out.println("Current offset is " + oam.offset());
                                } else {
                                    System.out.println("No committed offsets");
                                }
                                kafkaConsumer.seek(tp, offset);
                            }
                            tStart = System.currentTimeMillis();
                            if (start==0){
                                start=tStart;
                            }
                        }
                    });

                        while (true) {
                            ConsumerRecords<Integer, Bytes> poll = kafkaConsumer.poll(Duration.ofMillis(100L));
                            if (poll.count()>0){
                                Iterator<ConsumerRecord<Integer, Bytes>> iterator = poll.iterator();
                                while (iterator.hasNext()) {
                                    ConsumerRecord<Integer, Bytes> record = iterator.next();
                                    dataSize += record.value().get().length;
                                    dataNum++;
                                    if (dataNum >= 102400) {
                                        break;
                                    }
                                }
                                if (dataNum >= 102400) {
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        long tEnd = System.currentTimeMillis();
                        float time = (tEnd - tStart) / 1000f;
                        System.out.printf("\n%s Over send %d record , rate %.2f record/S , %.2f MB/S\n", Thread.currentThread().getName()
                                , 102400, 102400 / time, dataSize / (time * 1024 * 1024));
                        kafkaConsumer.close();
                    }
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
        long end = System.currentTimeMillis();
        float time = (end - start) / 1000f;
        System.out.printf("\nAll kafka producers Over send %d record , rate %.2f record/S , %.2f MB/S\n",
                102400 * KafkaConsumerNum, 102400 * KafkaConsumerNum / time, 8 * 100 * KafkaConsumerNum / time);
    }
}
