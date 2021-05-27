package Netty.LTPCClient;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/11/19 16:44
 */

import DisruptorQueue.*;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ExceptionHandlerWrapper;
import com.lmax.disruptor.dsl.ProducerType;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.protocol.types.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @program: BD_DAQ_InputSplit
 * @description:
 * @author: WuYe
 * @create: 2020-11-19 16:44
 **/
public class ClientNetty {

    // 要请求的服务器的ip地址
    private String ip;
    public static String topic;
    public static int kafkaBatchSize = 8192*3;
    // 服务器的端口
    private int port;
    public static int clientThread;
    public static int decoderPrintNum;
    private static int kafkaThread;
    private static int ringBufferSize;
    public static int trigger=1000000;
    private static int lengthFixed=1024*8;
    public static long startTime;
    public static HashMap<String, String> clientDataRateMap=new HashMap<>();
    public static HashMap<String, String> kafkaDataRateMap=new HashMap<>();
    private AtomicInteger connectNum = new AtomicInteger(0);
    private AtomicLong recordNumSum = new AtomicLong(0);
    private AtomicLong dataSizeSum = new AtomicLong(0L);
    private AtomicInteger connectNum1 = new AtomicInteger(0);
    private AtomicLong recordNumSum1 = new AtomicLong(0);
    private AtomicLong dataSizeSum1 = new AtomicLong(0L);
    private AtomicLong firstStartTime = new AtomicLong(0L);
    private AtomicLong firstStartTime1 = new AtomicLong(0L);
    public static ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

    public ClientNetty(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }


    private Disruptor<DataEvent> creatDisruptor() {
        Disruptor<DataEvent> disruptor = new Disruptor<>(new DataEventFactory(),
                ringBufferSize,
                new ThreadFactory() {
                    private int anInt = -1;

                    @Override
                    public Thread newThread(Runnable r) {
                        anInt++;
                        return new Thread(r, "KafkaProducer-" + anInt);
                    }
                },
                ProducerType.MULTI,
                new YieldingWaitStrategy()
        );
//        RingBuffer<DataEvent> ringBuffer = disruptor.getRingBuffer();
//        DisruptorProducer disruptorProducer = new DisruptorProducer(ringBuffer);
        //设置消费者线程
        setConsumers(disruptor);
        disruptor.start();
        return disruptor;
    }

    // 请求端主题
    private void action() {
        Disruptor<DataEvent> disruptor = creatDisruptor();


        EventLoopGroup workGroup = new NioEventLoopGroup(30);

        Bootstrap bs = new Bootstrap();
        try {
            bs.group(workGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.TCP_NODELAY,false)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) {
                            // 不定长解码器
//                            socketChannel.pipeline().addLast(
//                                    new MyProtocolDecoder(10000,
//                                            4,
//                                            4,
//                                            -8,
//                                            0,
//                                            true,
//                                            connectNum1, recordNumSum1, dataSizeSum1, firstStartTime1
//                                            ,new DisruptorProducer(disruptor.getRingBuffer()),
//                                            disruptor
//                                    ));


                            // 定长解码器
                            socketChannel.pipeline().addLast(
                                    new MyFixedLengthDecoder(
                                    lengthFixed, trigger,
                                    connectNum1, recordNumSum1, dataSizeSum1, firstStartTime1,disruptor,new DisruptorProducer(disruptor.getRingBuffer())
                            ));
                        }
                    });
            ArrayList<ChannelFuture> channelFutures = new ArrayList<>();
            // 客户端开启
            for (int i = 0; i < clientThread; i++) {
                channelFutures.add(bs.connect(ip, port));
            }
            int timeInterval=100;
//            service.scheduleAtFixedRate(new Runnable() {
//                private int second;
//                @Override
//                public void run() {
//                    long l = disruptor.getRingBuffer().getBufferSize() - disruptor.getRingBuffer().remainingCapacity();
//                    if (l>0) {
//                        System.out.printf("\n\n>>>>>>>>>%d second>>>>>>>>>>> RingBufferUsed: %d Element , %.2f%% , ConnectNum: %d ,recordSum: %d\n",
//                                second, l ,l*100f/disruptor.getRingBuffer().getBufferSize(),connectNum1.get(),recordNumSum1.get());
//                    }
//                    second+=timeInterval;
//                }
//            }, 0, timeInterval, TimeUnit.MILLISECONDS);

            // 等待直到连接中断
            for (ChannelFuture cf : channelFutures) {
                cf.channel().closeFuture().sync();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            workGroup.shutdownGracefully();
        }

    }

    private void setConsumers(Disruptor<DataEvent> disruptor) {
//        DataEventHandler[] consumers = new DataEventHandler[kafkaThread];
//        for (int i = 0; i < consumers.length; i++) {
//            consumers[i] = new DataEventHandler(topic);
//        }
//        disruptor.handleEventsWithWorkerPool(consumers);
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "hd08:9092,hd09:9092,hd10:9092,hd11:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProps.put("acks", "0");
        kafkaProps.put("buffer.memory", "335544320");
//        kafkaProps.put("retries", "0");
//        kafkaProps.put("linger.ms", "1");
        kafkaProps.put("batch.size",  ClientNetty.kafkaBatchSize+"");


        WorkProcessorDecorator[] workProcessorDecorators = new WorkProcessorDecorator[kafkaThread];
        RingBuffer<DataEvent> ringBuffer = disruptor.getRingBuffer();
        ExceptionHandler<DataEvent> exceptionHandler = new ExceptionHandlerWrapper<>();
        Sequence workSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        for (int i = 0; i < workProcessorDecorators.length; i++) {
            final SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
            DataEventHandler dataEventHandler = new DataEventHandler(topic, new KafkaProducer<>(kafkaProps));
            WorkProcessor<DataEvent> dataEventWorkProcessor = new WorkProcessor<>(
                    ringBuffer,
                    sequenceBarrier,
                    dataEventHandler,
                    exceptionHandler,
                    workSequence
            );
            dataEventHandler.setWorkProcessor(dataEventWorkProcessor);
            workProcessorDecorators[i] = new WorkProcessorDecorator(dataEventWorkProcessor,
                    dataEventHandler,
                    connectNum,
                    recordNumSum,
                    dataSizeSum,
                    firstStartTime
            );
        }
        disruptor.handleEventsWith(workProcessorDecorators);
    }

    public static void main(String[] args) {
        String host;
        if (args.length == 7) {
            host = args[0];
            clientThread = Integer.parseInt(args[1]);
            kafkaThread = Integer.parseInt(args[2]);
            topic = args[3];
            ringBufferSize =  Integer.parseInt(args[4]);
//            ringBufferSize = 1024 * 1024 * Integer.parseInt(args[4]);
            trigger = Integer.parseInt(args[5]);
            decoderPrintNum = trigger/Integer.parseInt(args[6]);
        } else {
            throw new RuntimeException("argc error;Please input parameter: host , clientThread ,kafkaThread,topic,ringBufferSize(MB),trigger,decoderPrintNum");
        }
//        if (args.length == 4) {
//            host = args[0];
//            clientThread = Integer.parseInt(args[1]);
//            trigger = Integer.parseInt(args[2]);
//            lengthFixed = Integer.parseInt(args[3]);
//        } else {
//            throw new RuntimeException("argc error;Please input parameter: host , clientThread ,trigger,lengthFixed");
//        }

        new ClientNetty(host, 20000).action();

    }

}
