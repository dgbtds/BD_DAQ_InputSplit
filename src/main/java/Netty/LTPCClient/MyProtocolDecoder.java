package Netty.LTPCClient;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/11/20 12:23
 */

import DisruptorQueue.DataEvent;
import DisruptorQueue.DisruptorProducer;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @program: BD_DAQ_InputSplit
 * @description:
 * @author: WuYe
 * @create: 2020-11-20 12:23
 **/
public class MyProtocolDecoder extends LengthFieldBasedFrameDecoder {

    private static final int HEADER_SIZE = 8;
    public static int maxPackLength = 8192;
    private final byte[] readBuffer =new byte[maxPackLength];
    private long startTime = 0;
    private int recordNum = 0;
    private int nullNum = 0;
    private long dataSize = 0;
    private AtomicInteger connectNum;
    private AtomicLong recordNumSum;
    private AtomicLong dataSizeSum;
    private AtomicLong firstStartTime;
    private DisruptorProducer disruptorProducer;
    private Disruptor<DataEvent> disruptor;
    private KafkaProducer<Integer, byte[]> kafkaProducer;


    /**
     * @param maxFrameLength      帧的最大长度
     * @param lengthFieldOffset   length字段偏移的地址
     * @param lengthFieldLength   length字段所占的字节长
     * @param lengthAdjustment    修改帧数据长度字段中定义的值，可以为负数 因为有时候我们习惯把头部记入长度,若为负数,则说明要推后多少个字段
     * @param initialBytesToStrip 解析时候跳过多少个长度
     * @param failFast            为true，当frame长度超过maxFrameLength时立即报TooLongFrameException异常，为false，读取完整个帧再报异
     * @param disruptorProducer
     * @param disruptor
     */

    public MyProtocolDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength, int lengthAdjustment, int initialBytesToStrip, boolean failFast,
                             AtomicInteger connectNum, AtomicLong recordNumSum, AtomicLong dataSizeSum, AtomicLong firstStartTime,
                             DisruptorProducer disruptorProducer, Disruptor<DataEvent> disruptor) {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip, failFast);
        this.connectNum = connectNum;
        this.recordNumSum = recordNumSum;
        this.dataSizeSum = dataSizeSum;
        this.firstStartTime = firstStartTime;
        this.disruptorProducer=disruptorProducer;
        this.disruptor=disruptor;
    }
    public MyProtocolDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength, int lengthAdjustment, int initialBytesToStrip, boolean failFast,
                             AtomicInteger connectNum, AtomicLong recordNumSum, AtomicLong dataSizeSum, AtomicLong firstStartTime
                             ) {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip, failFast);
        this.connectNum = connectNum;
        this.recordNumSum = recordNumSum;
        this.dataSizeSum = dataSizeSum;
        this.firstStartTime = firstStartTime;
    }
    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (recordNum==0){
            startTime = System.currentTimeMillis();
        }
        //在这里调用父类的方法
       in = (ByteBuf) super.decode(ctx, in);

        if (in == null) {
            nullNum++;
            return null;
        }

        //读取ladder字段
        int length = in.readableBytes();
//        if (length==16){
//            long end = System.currentTimeMillis();
//            long time = (end - startTime);
//            float rate = recordNum * 1000f / time;
//            float rateMB = dataSize / (1048.576f * time);
//            System.out.printf("\n%s Over split %d frame,size: %.2fMb , rate : %.2f splitRecord/S， %.2f MB/S ,Nullnum : %d\n", Thread.currentThread().getName()
//                    , recordNum,dataSize/1048576f, rate, rateMB,nullNum);
//
//            dataSizeSum.set(dataSizeSum.get() + dataSize);
//            recordNumSum.set(recordNum + recordNumSum.get());
//            ReferenceCountUtil.release(in);
//            ctx.close();
//            return null;
//        }
        long trailer = in.getUnsignedInt(length-4);
        if (trailer != 0x22334455) {
          throw new RuntimeException("包尾部错误");
        }
        dataSize += length;
        recordNum++;
        if (recordNum%100==0) {
//        if (recordNum%ClientNetty.decoderPrintNum==0) {
            long end = System.currentTimeMillis();
            long reS = recordNumSum.addAndGet(ClientNetty.decoderPrintNum);
//            float sumRate=reS*1000/(128f*(end-firstStartTime.get()));
//            System.out.printf("\n########Split########%s recv %d record , use time: %.2f s , rate: %.2f MB/s , sumRate: %.2f MB/s",
//                    Thread.currentThread().getName(),
//                    recordNum,(end-startTime)/1000f,ClientNetty.decoderPrintNum*1000/(128f*(end-startTime)),sumRate
//            );
            startTime=System.currentTimeMillis();
        }
//        ReferenceCountUtil.release(in);

        disruptorProducer.pushData(in);
        return null;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        int i = connectNum.incrementAndGet();
        if (i==1){
            recordNumSum.set(0);
            dataSizeSum.set(0);
            firstStartTime.set(System.currentTimeMillis());
        }
        System.out.printf("\n--------%s------------------channel Registered :%d \n ",
                Thread.currentThread().getName(), i);
        ctx.fireChannelRegistered();
    }

    private void printPckInfo(ByteBuf in){
        int ladder= (int) in.getUnsignedInt(0);
        int length=(int) in.getUnsignedInt(4);
        int trigger=(int) in.getUnsignedInt(8);
        System.out.printf("\nladder : %d , length : %d , trigger : %d , recordNum : %d ,record Nullnum : %d ",ladder,length,trigger,recordNum+1,nullNum);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
        int i = connectNum.decrementAndGet();
        if (i == 0) {
            long endAll = System.currentTimeMillis();
            float sizeAll=dataSizeSum.get()/1048576f;
            long useTime = endAll - firstStartTime.get();
            float rateAll = dataSizeSum.get() / (1048.576f *useTime);
            float raterecordAll = recordNumSum.get()*1000f / useTime;
            System.out.printf("\nall thread split %d frame , recv %.2f MB data ,useTime %ds, rate : %.2f splitRecord/S , %.2f MB/S \n",
                    recordNumSum.get(),sizeAll,useTime,raterecordAll ,rateAll);
            disruptor.shutdown();
        }
        ctx.fireChannelUnregistered();
    }
}

