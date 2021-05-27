package Netty.LTPCClient;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/11/20 12:34
 */

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteBufferSerializer;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @program: BD_DAQ_InputSplit
 * @description:
 * @author: WuYe
 * @create: 2020-11-20 12:34
 **/
public class KafkaProducerSend extends ChannelInboundHandlerAdapter {
    private KafkaProducer<Integer, byte[]> kafkaProducer;
    private String topic;
    private int recordNum = 0;
    private long startTime = 0;
    private long dataSize = 0;
    private AtomicInteger connectNum;
    private AtomicLong recordNumSum;
    private AtomicLong dataSizeSum;
    private AtomicLong firstStartTime ;

//    public KafkaProducerSend(KafkaProducer<Integer, byte[]> kafkaProducer, String topic, AtomicInteger connectNum, AtomicLong recordNumSum, AtomicReference<Float> rateSum) {
//        this.kafkaProducer = kafkaProducer;
//        this.topic = topic;
//        this.connectNum = connectNum;
//        this.recordNumSum = recordNumSum;
//        this.rateSum = rateSum;
//    }
    public KafkaProducerSend( String topic, AtomicInteger connectNum, AtomicLong recordNumSum, AtomicLong dataSizeSum,AtomicLong firstStartTime) {
        // 创建生产者
        Properties kafkaProps = new Properties();
        // 指定broker（这里指定了2个，1个备用），如果你是集群更改主机名即可，如果不是只写运行的主机名
        kafkaProps.put("bootstrap.servers", "hd14:9092,hd15:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProps.put("acks","0");
//        kafkaProps.put("retries", "0");
//        kafkaProps.put("linger.ms", "1");
        kafkaProps.put("batch.size", "16384");
        this.kafkaProducer = new KafkaProducer<>(kafkaProps);
        this.topic = topic;
        this.connectNum = connectNum;
        this.recordNumSum = recordNumSum;
        this.dataSizeSum = dataSizeSum;
        this.firstStartTime = firstStartTime;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf data = (ByteBuf) msg;
        if (data == null ) {
            //            if (nullNum%100==0) {
//                System.out.println(Thread.currentThread().getName()+" get null:"+nullNum);
//            }
            return;
        }
        if (data.readableBytes()==16){
            long end = System.currentTimeMillis();
            long time = (end - startTime);
            float rate = recordNum * 1000f / time;
            float rateSize = dataSize/ (1048.576f * time);
            System.out.printf("\n%s Over send %d record , rate %.2f record/S , %.2f MB/S\n", Thread.currentThread().getName()
                    , recordNum, rate,rateSize);
            dataSizeSum.set(dataSizeSum.get() + dataSize);
            recordNumSum.set(recordNum + recordNumSum.get());
            kafkaProducer.close();
            ctx.close();
            return;
        }
        byte[] arr;
        if (data.hasArray()) {
            arr = data.array();
        } else {
            arr = new byte[data.readableBytes()];
            data.readBytes(arr);
        }
        ReferenceCountUtil.release(data);
        kafkaProducer.send(new ProducerRecord<>(topic, arr));
        recordNum++;
        dataSize+=arr.length;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        int i = connectNum.incrementAndGet();
        if (i==1){
            recordNumSum.set(0);
            dataSizeSum.set(0);
            firstStartTime.set(System.currentTimeMillis());
        }
        startTime = System.currentTimeMillis();
//        System.out.printf("\n--------%s------------------channel Registered :%d ",
//                Thread.currentThread().getName(), i);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        int i = connectNum.decrementAndGet();
        if (i == 0) {
            long endAll = System.currentTimeMillis();
            long timeAll = (endAll - firstStartTime.get());
            float rateRecordAll = recordNumSum.get()*1000f /timeAll;
            float rateSizeAll = dataSizeSum.get() / (1048.576f * timeAll);
            System.out.printf("\n\nall thread send %d kafka record , rate : %.2f record/S , %.2f MB/S \n",
                    recordNumSum.get(),rateRecordAll ,rateSizeAll);
        }
    }
}
