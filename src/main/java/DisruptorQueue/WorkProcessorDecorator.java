package DisruptorQueue;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/12/1 21:37
 */

import Netty.LTPCClient.ClientNetty;
import com.lmax.disruptor.*;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @program: BD_DAQ_InputSplit
 *
 * @description:
 *
 * @author: WuYe
 *
 * @create: 2020-12-01 21:37
 **/
public class WorkProcessorDecorator implements EventProcessor {
    private WorkProcessor<DataEvent> workProcessor;
    private DataEventHandler dataEventHandler;
    private AtomicInteger consumerNums;
    private AtomicLong recordNumSum ;
    private AtomicLong dataSizeSum ;
    private AtomicLong firstStartTime;
    private long start;

    public WorkProcessorDecorator(WorkProcessor<DataEvent> workProcessor, DataEventHandler dataEventHandler,
                                  AtomicInteger consumerNums, AtomicLong recordNumSum,
                                  AtomicLong dataSizeSum, AtomicLong firstStartTime) {
        this.workProcessor = workProcessor;
        this.dataEventHandler = dataEventHandler;
        this.consumerNums = consumerNums;
        this.recordNumSum = recordNumSum;
        this.dataSizeSum = dataSizeSum;
        this.firstStartTime = firstStartTime;
    }

    @Override
    public Sequence getSequence() {
        return workProcessor.getSequence();
    }

    @Override
    public void halt() {
        workProcessor.halt();
    }

    @Override
    public boolean isRunning() {
        return workProcessor.isRunning();
    }
    private void consumerRegister(){
        int i = consumerNums.incrementAndGet();
        System.out.printf("\n--------%s------------------Consumer Registered :%d \n ",
                Thread.currentThread().getName(), i);
        if (i==1){
            firstStartTime.set(System.currentTimeMillis());
        }
    }
    private void consumerUnRegister(){
        recordNumSum.set(recordNumSum.get()+dataEventHandler.getRecordNum());
        dataSizeSum.set(dataSizeSum.get()+dataEventHandler.getDataSize());
        if (consumerNums.decrementAndGet()==0){
            long end = System.currentTimeMillis();
            float timeAll = (end - firstStartTime.get())/1000f;
            float rateRecordAll = recordNumSum.get()  / timeAll;
            float rateMBAll = dataSizeSum.get()  / (1048576*timeAll);
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^Kafka send Over^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
            for(String s:ClientNetty.kafkaDataRateMap.values()){
                stringBuffer.append(s);
            }
            String format = String.format("\nall Kafka Producer thread send %d kafka record , messageRate: %.2f kafkaRecord/S , DataRate: %.2f MB/s \n",
                    recordNumSum.get(), rateRecordAll,rateMBAll);
            stringBuffer.append(format);
            stringBuffer.append("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^Kafka send Over^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n");
            System.out.println(stringBuffer.toString());
        }
        ClientNetty.service.shutdown();
    }

    @Override
    public void run() {
        consumerRegister();
        start=System.currentTimeMillis();
        workProcessor.run();
        long end=System.currentTimeMillis();
        long time=(end-start)/1000;
        String format = String.format(
                "\n %s Complete&Send %d kafka record , useTime: %ds , messageRate: %.2f kafkaRecord/S , DataRate: %.2f MB/s \n",
                Thread.currentThread().getName(), dataEventHandler.getRecordNum(), time,
                dataEventHandler.getRecordNum() / (float) time, dataEventHandler.getDataSize() / (1048576f * time));
        ClientNetty.kafkaDataRateMap.put(Thread.currentThread().getName(),format);
        consumerUnRegister();
    }
}
