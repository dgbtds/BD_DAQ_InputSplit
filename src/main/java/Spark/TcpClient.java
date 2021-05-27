package Spark;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/10/29 21:47
 */

import Input.InputRecord;
import Util.IntByteConvert;
import Util.LineReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: BG_DAQ_FrameWork
 * @description:
 * @author: WuYe
 * @create: 2020-10-29 21:47
 **/
public class TcpClient {
    private static int maxPckLength = 1024;
    private static int kafkaProduceThreadNum = 0;
    private static String host;
    private static String topic;
    private static int topicParttionNum;
    public static int trigger;
    public static int pckLength;
    private static int clientThread;
    private static int buffersize = 64 * 1024;
    private static int remotePort = 20000;
    private static ExecutorService tcpClientService;
    private static ExecutorService kafkaProducerService;
    public static AtomicInteger rateSum=new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
            kafkaProduceThreadNum = 0;
        if (args.length == 4) {
            host = args[0];
            clientThread = Integer.parseInt(args[1]);
            trigger =  Integer.parseInt(args[2]);
            pckLength = Integer.parseInt(args[3]);
        } else {
            throw new RuntimeException("argc error;Please input parameter: host , clientThread ,trigger,pckLength");
        }
        byte[] delimiter = IntByteConvert.intToByteArray(0x22334455);
        tcpClientService = Executors.newFixedThreadPool(clientThread);
        if (kafkaProduceThreadNum > 0) {
            kafkaProduceThreadNum=clientThread;
            kafkaProducerService = Executors.newFixedThreadPool(kafkaProduceThreadNum);
        }
        new TcpClient().inputService(host, topic, delimiter);

    }

    public void inputService(String host, String topic, byte[] recordDelimiterBytes) throws ExecutionException, InterruptedException {
        for (int i = 0; i < clientThread; i++) {
            LinkedBlockingQueue<InputRecord<Integer, byte[]>> dataQueue = new LinkedBlockingQueue<>();
            tcpClientService.submit(new recvFixedLengthPck(host, remotePort, pckLength, trigger));
            for (int j = 0; j < kafkaProduceThreadNum; j++) {
                kafkaProducerService.submit(new KafkaProduceTask(topic, remotePort, dataQueue));
            }
        }
        tcpClientService.shutdown();
        if (kafkaProducerService != null) {
            kafkaProducerService.shutdown();
        }
        tcpClientService.awaitTermination(10,TimeUnit.MINUTES);
        System.out.println("\nrecv over all rate sum is: "+rateSum.get()+"MByte/s.");
    }
    class recvFixedLengthPck implements Runnable{
        public String host;
        public int port;
        public int fixedLength;
        public int trigger;
        public LinkedList<byte[]> byteList;

        public recvFixedLengthPck(String host, int port, int fixedLength, int trigger) {
            this.host = host;
            this.port = port;
            this.fixedLength = fixedLength;
            this.trigger = trigger;
            this.byteList= new LinkedList<>();
        }

        @Override
        public void run() {
            Socket socket = null;
            long packageNumber = 0;
            float rate;
            long start = System.currentTimeMillis();
            InputStream inputStream = null;
            try {
                socket = new Socket(host, port);
                String localHostName = InetAddress.getLocalHost().getHostName();
                System.out.println("connect to remote host : " + host + " , port:" + port);
                System.out.println("localHost is : " + localHostName);
                inputStream = socket.getInputStream();
                int offset;
                for(int i=0;i<trigger;i++){
                    byte[] bytes = new byte[fixedLength];
                    offset=0;
                    while (offset<fixedLength){
                        int read = inputStream.read(bytes, offset, fixedLength - offset);
                        if (read==-1){
                            offset=read;
                            break;
                        }
                        offset+=read;
                    }
                    if (offset==-1){
                        System.out.println("read "+i+" pck inputStream closed");
                        break;
                    }
                    byteList.add(bytes);
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (inputStream!=null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                long end = System.currentTimeMillis();
                rate = trigger*pckLength / (1048.576f * (end - start));
                System.out.printf("\n#######data record split : %d, %.2fMB" +
                                ",use time : %.2f S,split data rate:%.2f MB/s  \n",
                        trigger, trigger*pckLength / 1048576f,(end - start) / 1000f, rate);
                rateSum.addAndGet((int) rate);
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    class splitRecordTask implements Callable<Float> {
        private String host;
        private int port;
        private byte[] recordDelimiterBytes;
        private LinkedBlockingQueue<InputRecord<Integer, byte[]>> dataQueue;

        public splitRecordTask(String host, int port, byte[] recordDelimiterBytes, LinkedBlockingQueue<InputRecord<Integer, byte[]>> dataQueue) {
            this.host = host;
            this.port = port;
            this.recordDelimiterBytes = recordDelimiterBytes;
            this.dataQueue = dataQueue;
        }

        @Override
        public Float call() {
            Socket socket = null;
            long packageNumber = 0;
            float rate;
            long start = System.currentTimeMillis();
            try {
                socket = new Socket(host, port);
                String localHostName = InetAddress.getLocalHost().getHostName();
                System.out.println("connect to remote host : " + host + " , port:" + port);
                System.out.println("localHost is : " + localHostName);
                byte[] checkCode=new byte[10];
                LineReader lineReader = new LineReader(socket.getInputStream(), buffersize, recordDelimiterBytes, checkCode);
                packageNumber = lineReader.readLineToQueue(maxPckLength, dataQueue);
//                packageNumber = lineReader.readFixedLengthRecord(maxPckLength, dataQueue, model);
//
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                long end = System.currentTimeMillis();
                rate = (float) packageNumber / (1.024f * (end - start));
                System.out.printf("\n#######data record split : %d" +
                                ",use time : %.2f S,split data rate:%.2f MB/s  \n",
                        packageNumber, (end - start) / 1000f, rate);
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return rate;
        }
    }

    static class KafkaProduceTask implements Callable<Float> {
        private String topic;
        private int port;
        private LinkedBlockingQueue<InputRecord<Integer, byte[]>> dataQueue;

        public KafkaProduceTask(String topic, int port, LinkedBlockingQueue<InputRecord<Integer, byte[]>> dataQueue) {
            this.topic = topic;
            this.port = port;
            this.dataQueue = dataQueue;
        }

        @Override
        public Float call() {
            long start = System.currentTimeMillis();
            // 创建生产者
            Properties kafkaProps = new Properties();
            // 指定broker（这里指定了2个，1个备用），如果你是集群更改主机名即可，如果不是只写运行的主机名
            kafkaProps.put("bootstrap.servers", "hd14:9092,hd15:9092");
            kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
            kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            int recordNum = 0;
            int parttionNum = 0;
            float rate;
            KafkaProducer<Integer, byte[]> kafkaProducer = null;
            try {
                kafkaProducer = new KafkaProducer<>(kafkaProps);
                while (true) {
                    InputRecord<Integer, byte[]> poll = dataQueue.poll(10, TimeUnit.SECONDS);
                    if (poll != null) {
                        recordNum++;
                        if (recordNum % 10000 == 0) {
                            System.out.printf("%s , send %d record , queue length is %d:\n", Thread.currentThread().getName()
                                    , recordNum, dataQueue.size());
                        }
                        kafkaProducer.send(new ProducerRecord<>(topic, parttionNum, poll.getKey(), poll.getValue()));
                        if (parttionNum == topicParttionNum - 1) {
                            parttionNum = 0;
                        } else {
                            parttionNum++;
                        }
                    } else {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                if (kafkaProducer != null) {
                    kafkaProducer.close();
                }
                long end = System.currentTimeMillis() - 10000;
                rate = (float) recordNum * 1000 / (end - start);
                System.out.printf("\nthread: %s ,send %d kafkaRecords\n,use Time:%.2f S , " +
                                "recordRate : %.0f \n",
                        Thread.currentThread().getName(), recordNum, (end - start) / 1000f, rate);
            }

            return rate;
        }
    }
}

