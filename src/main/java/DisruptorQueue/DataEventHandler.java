package DisruptorQueue;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/12/1 16:20
 */

import Netty.LTPCClient.ClientNetty;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkProcessor;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @program: BD_DAQ_InputSplit
 * @description:
 * @author: WuYe
 * @create: 2020-12-01 16:20
 **/
public class DataEventHandler implements WorkHandler<DataEvent> {
    private String topic;
    private long recordNum = 0;
    private long dataSize = 0;
    private long startTime;
    public KafkaProducer<Integer, byte[]> kafkaProducer;
    private WorkProcessor<DataEvent> workProcessor;
    private byte[] arr = new byte[1024*8];

    public DataEventHandler(String topic, KafkaProducer<Integer, byte[]> kafkaProducer) {
        this.topic = topic;
        this.kafkaProducer = kafkaProducer;
    }

    public WorkProcessor<DataEvent> getWorkProcessor() {
        return workProcessor;
    }

    public void setWorkProcessor(WorkProcessor<DataEvent> workProcessor) {
        this.workProcessor = workProcessor;
    }

    @Override
    public void onEvent(DataEvent event) {
        if (recordNum == 0) {
            startTime = System.currentTimeMillis();
        }
        ByteBuf data = event.getByteBuf();
        int length = data.readableBytes();
        data.getBytes(0,arr);
        data.release();
        recordNum++;
        if (recordNum%ClientNetty.decoderPrintNum==0){
            long saveValue=recordNum;
            kafkaProducer.send(new ProducerRecord<>(topic, arr), (metadata, exception) -> {
                if(exception != null) {
                    exception.printStackTrace();
                    return;
                }
                if (metadata!= null){
                    long end = System.currentTimeMillis();
                    System.out.printf("\n%s send %d record , use time: %.2f s , messageRate: %.2f record/s , dataRate: %.2f MB/s ",
                            Thread.currentThread().getName(), saveValue, (end - startTime) / 1000f,
                            saveValue * 1000f/ (end - startTime),saveValue*arr.length/(1024*1.024f*(end - startTime))
                    );
                    System.out.println("");
                }
                else {
                    System.out.println("metadata== null,send kafka message error!");
                }
            });
        }else {
            kafkaProducer.send(new ProducerRecord<>(topic,arr));
        }
        dataSize += length;
    }

    public long getRecordNum() {
        return recordNum;
    }

    public long getDataSize() {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public KafkaProducer<Integer, byte[]> getKafkaProducer() {
        return kafkaProducer;
    }

    public void setKafkaProducer(KafkaProducer<Integer, byte[]> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }
}
