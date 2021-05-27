package DisruptorQueue;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/12/1 17:14
 */

import com.lmax.disruptor.RingBuffer;
import io.netty.buffer.ByteBuf;

/**
 * @program: BD_DAQ_InputSplit
 *
 * @description:
 *
 * @author: WuYe
 *
 * @create: 2020-12-01 17:14
 **/
public class DisruptorProducer {
    private final RingBuffer<DataEvent> ringBuffer;

    public DisruptorProducer(RingBuffer<DataEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void pushData(ByteBuf byteBuf){
        long sequence = ringBuffer.next();
        try{
            DataEvent event = ringBuffer.get(sequence);
            event.setByteBuf(byteBuf);
        }finally {
            ringBuffer.publish(sequence);
        }
    }
}
