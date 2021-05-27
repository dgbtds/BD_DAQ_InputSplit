package Netty.LTPCClient;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/12/28 19:03
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @program: BD_DAQ_InputSplit
 *
 * @description:
 *
 * @author: WuYe
 *
 * @create: 2020-12-28 19:03
 **/
public class KafkaSendTest {
    public static void main(String[] args) throws InterruptedException {
        int KafkaProducerNum;
        int messageLengthKb;
        if (args.length == 2) {
            KafkaProducerNum = Integer.parseInt(args[0]);
            messageLengthKb = Integer.parseInt(args[1]);
        }
        else {
            throw new RuntimeException("argc error;Please input parameter: KafkaProducerNum,messageLength");
        }
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "hd08:9092,hd10:9092,hd13:9092,hd14:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProps.put("compression.type ", "none");
        kafkaProps.put("acks", "0");
//        kafkaProps.put("retries", "0");
//        kafkaProps.put("linger.ms", "1");
        kafkaProps.put("batch.size", "24576");
        ExecutorService executorService = Executors.newFixedThreadPool(KafkaProducerNum);
        byte[] arr=new byte[messageLengthKb*1024];
        long start=System.currentTimeMillis();
        for(int i=0;i<KafkaProducerNum;i++){
            executorService.submit(new Runnable() {
                long tStart=System.currentTimeMillis();
                @Override
                public void run() {
                    KafkaProducer<Integer, byte[]> kafkaProducer = new KafkaProducer<>(kafkaProps);
                    System.out.println(Thread.currentThread().getName());
                    for(int i=0;i<102400;i++){
                        kafkaProducer.send(new ProducerRecord<>("Maps1",i%15,i,arr));
                    }
                    kafkaProducer.flush();
                    kafkaProducer.close();
                    long tEnd=System.currentTimeMillis();
                    float time = (tEnd - tStart) / 1000f;
                    System.out.printf("\n%s Over send %d record , rate %.2f record/S , %.2f MB/S\n", Thread.currentThread().getName()
                            , 102400, 102400/time,messageLengthKb*100/time);
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
//        KafkaProducer<Integer, byte[]> kafkaProducer = new KafkaProducer<>(kafkaProps);
//        for(int i=0;i<10;i++){
//            kafkaProducer.send(new ProducerRecord<>("Maps1", arr));
//        }
//        kafkaProducer.flush();
//        kafkaProducer.close();
        long end=System.currentTimeMillis();
        float time = (end - start) / 1000f;
        System.out.printf("\nAll kafka producers Over send %d record , rate %.2f record/S , %.2f MB/S\n",
                102400*KafkaProducerNum, 102400*KafkaProducerNum/time,messageLengthKb*100*KafkaProducerNum/time);
    }
}
