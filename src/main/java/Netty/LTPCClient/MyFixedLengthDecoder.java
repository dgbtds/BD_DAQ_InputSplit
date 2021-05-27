package Netty.LTPCClient;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/11/30 19:14
 */

import DisruptorQueue.DisruptorProducer;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.FixedLengthFrameDecoder;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @program: BD_DAQ_InputSplit
 * @description:
 * @author: WuYe
 * @create: 2020-11-30 19:14
 **/
public class MyFixedLengthDecoder extends FixedLengthFrameDecoder {
    private long startTime = 0;
    private long recordNum = 0;
    private long dataSize = 0;
    private int trigger;
    private int nullNum;
    private int lengthFixed;
    private AtomicInteger connectNum;
    private AtomicLong recordNumSum;
    private AtomicLong dataSizeSum;
    private AtomicLong firstStartTime;
    private Disruptor disruptor;
    private DisruptorProducer disruptorProducer;

    public MyFixedLengthDecoder(int frameLength, int trigger, AtomicInteger connectNum, AtomicLong recordNumSum, AtomicLong dataSizeSum, AtomicLong firstStartTime,Disruptor disruptor,DisruptorProducer disruptorProducer) {
        super(frameLength);
        this.lengthFixed=frameLength;
        this.connectNum = connectNum;
        this.trigger = trigger;
        this.recordNumSum = recordNumSum;
        this.dataSizeSum = dataSizeSum;
        this.firstStartTime = firstStartTime;
        this.disruptor=disruptor;
        this.disruptorProducer=disruptorProducer;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        in = (ByteBuf) super.decode(ctx, in);
        if (in==null){
            nullNum++;
            return null;
        }
        if (recordNum==0) {
            startTime = System.currentTimeMillis();
        }
        recordNum++;
        if (recordNum <= trigger) {
            dataSize += lengthFixed;
            if (recordNum == trigger) {
                long end = System.currentTimeMillis();
                long time = (end - startTime);
                float rate = recordNum * 1000f / time;
                float rateMB = dataSize / (1048.576f * time);
                dataSizeSum.set(dataSizeSum.get() + dataSize);
                recordNumSum.set(recordNumSum.get() + recordNum);
               String format = String.format("\n%s Complete&split %d frame,size: %.2fMb , frameRate: %.2f splitRecord/S， DataRate: %.2f MB/S\n", Thread.currentThread().getName()
                        , recordNum, dataSize / 1048576f, rate, rateMB);
                ClientNetty.clientDataRateMap.put(Thread.currentThread().getName(),format);
                ctx.close();
            } else if (recordNum % ClientNetty.decoderPrintNum == 0) {
                //        else{
                long end = System.currentTimeMillis();
                long time = (end - startTime);
                float rate = recordNum * 1000f / time;
                float rateMB = dataSize / (1048.576f * time);
                System.out.printf("\n%s ReadOut&split %d frame , size: %.2fMb , useTimer: %ds , frameRate: %.2f splitRecord/S， DataRate: %.2f MB/S, nullNum:%d\n", Thread.currentThread().getName()
                        , recordNum, dataSize / 1048576f, time / 1000, rate, rateMB, nullNum);

            }

            disruptorProducer.pushData(in);
        }else {
            ReferenceCountUtil.release(in);
            if (recordNumSum.get()>=trigger*ClientNetty.clientThread) {
                disruptor.shutdown();
            }
        }
        return null;
//        if (in.readableBytes() == lengthFixed) {
//            recordNum++;
//            dataSize+=lengthFixed;
//            recordNumSum.incrementAndGet();
//            dataSizeSum.addAndGet(lengthFixed);
////            long trailer =  in.getUnsignedInt( in.readableBytes()-4);
////            if (trailer != 0x22334455) {
////                throw new Exception("包尾部错误");
////            }
//            if (recordNum == 1024000) {
//                long end = System.currentTimeMillis();
//                long time = (end - startTime);
//                float rate = recordNum * 1000f / time;
//                float rateMB = dataSize / (1048.576f * time);
//                System.out.printf("\n%s Over split %d frame,size: %.2fMb , rate : %.2f splitRecord/S， %.2f MB/S, nullNum:%d\n", Thread.currentThread().getName()
//                        , recordNum, dataSize / 1048576f, rate, rateMB,nullNum);
//                ctx.close();
//            }
//            ReferenceCountUtil.release(in);
//            return null;
//        } else {
//            ReferenceCountUtil.release(in);
//           System.out.println("length error :" + in.readableBytes());
//           return null;
//        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        int i = connectNum.incrementAndGet();
        if (i == 1) {
            recordNumSum.set(0);
            dataSizeSum.set(0);
            firstStartTime.set(System.currentTimeMillis());
        }
        if (i==ClientNetty.clientThread){
            ClientNetty.startTime=System.currentTimeMillis();
        }
        System.out.printf("\n--------%s------------------channel Registered :%d \n ",
                Thread.currentThread().getName(), i);
        ctx.fireChannelRegistered();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
        int i = connectNum.decrementAndGet();
        System.out.printf("\n--------%s------------------channel Unregistered :%d \n",
                Thread.currentThread().getName(), i);
        if (i == 0) {
            long endAll = System.currentTimeMillis();
            float sizeMBAll = dataSizeSum.get() / 1048576f;
            float rateAll = dataSizeSum.get() / (1048.576f * (endAll - firstStartTime.get()));
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^Split Over^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
            for(String s:ClientNetty.clientDataRateMap.values()){
                stringBuffer.append(s);
            }
            String format = String.format("\nall thread split %d frame , recv %.2f MB data , rate : %.2f MB/S \n",
                    recordNumSum.get(), sizeMBAll, rateAll);
            stringBuffer.append(format);
            stringBuffer.append("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^Split Over^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n");
            System.out.println(stringBuffer.toString());
        }
        ctx.fireChannelUnregistered();
    }
}
