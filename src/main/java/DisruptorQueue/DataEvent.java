package DisruptorQueue;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/12/1 16:16
 */

import io.netty.buffer.ByteBuf;

/**
 * @program: BD_DAQ_InputSplit
 *
 * @description:
 *
 * @author: WuYe
 *
 * @create: 2020-12-01 16:16
 **/
public class DataEvent {
    private ByteBuf byteBuf;


    public ByteBuf getByteBuf() {
        return byteBuf;
    }

    public void setByteBuf(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }
}
