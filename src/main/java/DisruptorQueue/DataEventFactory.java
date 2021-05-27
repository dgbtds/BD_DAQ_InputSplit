package DisruptorQueue;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/12/1 16:17
 */

import com.lmax.disruptor.EventFactory;

/**
 * @program: BD_DAQ_InputSplit
 *
 * @description:
 *
 * @author: WuYe
 *
 * @create: 2020-12-01 16:17
 **/
public class DataEventFactory implements EventFactory<DataEvent> {
    @Override
    public DataEvent newInstance() {
        return new DataEvent();
    }
}
