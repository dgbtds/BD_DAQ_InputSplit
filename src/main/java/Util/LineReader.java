package Util;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/10/30 19:09
 */


import Input.InputRecord;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @program: BG_DAQ_FrameWork
 * @description:
 * @author: WuYe
 * @create: 2020-10-30 19:09
 **/
public class LineReader implements Closeable {
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private int bufferSize;
    private InputStream in;
    private byte[] buffer;
    // the number of bytes of real data in the buffer
    private int bufferLength = 0;
    // the current position in the buffer

    private int bufferPosn = 0;
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    // The line delimiter
    private final byte[] recordDelimiterBytes;
    private final byte[] checkCode;

    /**
     * Create a line reader that reads from the given stream using the
     * given buffer-size.
     *
     * @param in         The input stream
     * @param bufferSize Size of the read buffer
     * @param checkCode
     * @throws IOException
     */
    public LineReader(InputStream in, int bufferSize, byte[] recordDelimiterBytes, byte[] checkCode) {
        this.in = in;
        this.bufferSize = bufferSize;
        this.checkCode = checkCode;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    public LineReader(InputStream in, byte[] recordDelimiterBytes, byte[] checkCode) {
        this.in = in;
        this.checkCode = checkCode;
        this.bufferSize = DEFAULT_BUFFER_SIZE;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    /**
     * Close the underlying stream.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        in.close();
    }

    public InputRecord<Integer, byte[]> readLineToRecord(int maxPckLength) throws IOException {
        return readCustomLine2(maxPckLength);
    }

    public long readLineToQueue(int maxPckLength, LinkedBlockingQueue<InputRecord<Integer, byte[]>> dataQueue) throws IOException, InterruptedException {
        return readCustomLine1(maxPckLength, dataQueue);
    }

    public long decode(int maxPckLength, LinkedBlockingQueue<byte[]> dataQueue, int offset, int pcklengthCodeByteNum) throws IOException, InterruptedException {
        int bytesConsumed = 0;
        int delPosn = 0;
        int pckLength = 0;
        int ambiguousByteCount = 0; // To capture the ambiguous characters count
        byte[] bytes = new byte[maxPckLength];
        do {
            int startPosn = bufferPosn; // Start from previous end position
            if (bufferPosn > bufferLength) {
                startPosn = bufferPosn = 0;
                bufferLength = fillAllBuffer(in, buffer);
                if (bufferLength <= bufferSize) {
                    break;
                }
            }
            //find delemiter
            for (; bufferPosn < bufferLength; ++bufferPosn) {
                if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
                    delPosn++;
                    if (delPosn >= recordDelimiterBytes.length) {
                        bufferPosn++;
                        break;
                    }

                } else if (delPosn != 0) {
                    delPosn = 0;
                    bufferPosn--;
                }
            }
            if (bufferPosn - startPosn > recordDelimiterBytes.length) {
                System.out.println("inputStream not start with header,error length:" + (bufferPosn - startPosn - recordDelimiterBytes.length));
            }
            //getPckLength
            if (bufferLength - bufferPosn >= pcklengthCodeByteNum + offset) {
                bufferPosn+=offset;
                for(int i=0;i<pcklengthCodeByteNum;i++){
                    pckLength|=(buffer[bufferPosn+i]&0xff)<<(8*(pcklengthCodeByteNum-i));
                }
                bufferPosn+=pcklengthCodeByteNum;
            }
            else if (bufferLength - bufferPosn > offset){
                int readPckLengthNum=0;
                bufferPosn+=offset;
                while (readPckLengthNum<=pcklengthCodeByteNum && bufferPosn<bufferLength){
                    pckLength|=(buffer[bufferPosn]&0xff)<<(8*(pcklengthCodeByteNum-readPckLengthNum+1));
                    bufferPosn++;
                    readPckLengthNum++;
                }
                for(;startPosn<bufferPosn;startPosn++){
                    bytes[bytesConsumed]=buffer[startPosn];
                    bytesConsumed++;
                }
                bufferLength= fillAllBuffer(in, buffer);
                startPosn=bufferPosn=0;
                if (bufferLength<pcklengthCodeByteNum-readPckLengthNum){
                    bufferPosn=bufferLength;
                    for(;startPosn<bufferPosn;startPosn++){
                        bytes[bytesConsumed]=buffer[startPosn];
                        bytesConsumed++;
                    }
                    break;
                }
                else {
                    while (readPckLengthNum<=pcklengthCodeByteNum){
                        pckLength|=(buffer[bufferPosn]&0xff)<<(8*(pcklengthCodeByteNum-readPckLengthNum+1));
                        bufferPosn++;
                        readPckLengthNum++;
                    }
                }
            }

            //get data
            if (bufferLength-startPosn>pckLength-bytesConsumed){
                bufferPosn=startPosn+pckLength-bytesConsumed;
                for(;startPosn<bufferPosn;startPosn++){
                    bytes[bytesConsumed]=buffer[startPosn];
                    bytesConsumed++;
                }
            }
            else {
                bufferPosn=bufferLength;
                for(;startPosn<bufferPosn;startPosn++){
                    bytes[bytesConsumed]=buffer[startPosn];
                    bytesConsumed++;
                }
                bufferLength=fillAllBuffer(in,buffer);

            }


        } while (delPosn < recordDelimiterBytes.length
                && bytesConsumed < maxPckLength);
        return 0;
    }

    public long readFixedLengthRecord(int pckLength, LinkedBlockingQueue<byte[]> dataQueue, boolean model) throws IOException, InterruptedException {
        int read = 0;
        int offset;
        int packageNum = 0;
        while (read != -1) {
            byte[] bytes = new byte[pckLength];
            offset = 0;
            while (offset < pckLength) {
                read = in.read(bytes, offset, pckLength - offset);
                if (read <= -1) {
                    break;
                }
                offset += read;
            }


            if (read > 0) {
                if (offset != pckLength) {
                    System.out.println("read != pckLength--->:" + offset);
                }
                packageNum++;
                if (model) {
                    dataQueue.put(bytes);
                }
            }
//            if (packageNum % 10000 == 0) {
//                System.out.printf("%s , recvPackageNum :%d , queue length :%d \n", Thread.currentThread().getName()
//                        , packageNum, dataQueue.size());
//            }

        }

        return packageNum;
    }

    private long readCustomLine1(int maxPckLength, LinkedBlockingQueue<InputRecord<Integer, byte[]>> dataQueue) throws IOException, InterruptedException {
        byte[] midBuffer = new byte[maxPckLength];
        boolean streamEof = false;
        long packageNumber = 0;
        while (!streamEof) {
            long bytesConsumed = 0;
            int midBuffer_pos = 0;
            int delPosn = 0;
            int ambiguousByteCount = 0; // To capture the ambiguous characters count
            do {
                int startPosn = bufferPosn; // Start from previous end position
                if (bufferPosn >= bufferLength) {
                    startPosn = bufferPosn = 0;
                    bufferLength = fillBuffer(in, buffer, ambiguousByteCount > 0);
                    if (bufferLength <= 0) {
                        for (int i = 0; i < ambiguousByteCount; i++, midBuffer_pos++) {
                            midBuffer[midBuffer_pos] = recordDelimiterBytes[i];
                        }
                        streamEof = true;
                        break; // EOF
                    }
                }
                //find delimiter
                for (; bufferPosn < bufferLength; ++bufferPosn) {
                    if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
                        delPosn++;
                        if (delPosn >= recordDelimiterBytes.length) {
                            bufferPosn++;
                            break;
                        }
                    } else if (delPosn != 0) {
                        bufferPosn--;
                        delPosn = 0;
                    }
                }
                int readLength = bufferPosn - startPosn;
                bytesConsumed += readLength;
                int appendLength = readLength - delPosn;
                if (appendLength > 0) {
                    if (ambiguousByteCount > 0) {
                        for (int i = 0; i < ambiguousByteCount; i++, midBuffer_pos++) {
                            midBuffer[midBuffer_pos] = recordDelimiterBytes[i];
                        }
                        //appending the ambiguous characters (refer case 2.2)
                        bytesConsumed += ambiguousByteCount;
                        ambiguousByteCount = 0;
                    }
                    for (int i = startPosn; i < startPosn + appendLength; i++, midBuffer_pos++) {
                        midBuffer[midBuffer_pos] = buffer[i];
                    }

                }
                if (bufferPosn >= bufferLength) {
                    if (delPosn > 0 && delPosn < recordDelimiterBytes.length) {
                        ambiguousByteCount = delPosn;
                        bytesConsumed -= ambiguousByteCount; //to be consumed in next
                    }
                }
            } while (delPosn < recordDelimiterBytes.length
                    && bytesConsumed < maxPckLength);
            if (bytesConsumed > Integer.MAX_VALUE) {
                throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
            }
            if (midBuffer_pos==12){
                System.out.println("recv 16 fin pck");
                break;
            }
            if (midBuffer_pos > 0) {
                dataQueue.put(new InputRecord<>(midBuffer_pos, midBuffer));
                packageNumber++;
            }
        }
        return packageNumber;
    }

    protected int fillAllBuffer(InputStream in, byte[] buffer)
            throws IOException {
        int offset = 0;
        int read;
        while (offset < bufferSize) {
            read = in.read(buffer, offset, bufferSize - offset);
            if (read <= 0) {
                break;
            }
            offset += read;
        }
        return offset;
    }
    protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter)
            throws IOException {
        return in.read(buffer);
    }


    private InputRecord<Integer, byte[]> readCustomLine2(int maxPckLength)
            throws IOException {
        long bytesConsumed = 0;
        int midBuffer_pos = 0;
        int delPosn = 0;
        int ambiguousByteCount = 0; // To capture the ambiguous characters count
        byte[] midBuffer = new byte[maxPckLength];
        do {
            int startPosn = bufferPosn; // Start from previous end position
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                bufferLength = fillBuffer(in, buffer, ambiguousByteCount > 0);
                if (bufferLength <= 0) {
                    for (int i = 0; i < ambiguousByteCount; i++, midBuffer_pos++) {
                        midBuffer[midBuffer_pos] = recordDelimiterBytes[i];
                    }
                    break; // EOF
                }
            }
            //find delimiter
            for (; bufferPosn < bufferLength; ++bufferPosn) {
                if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
                    delPosn++;
                    if (delPosn >= recordDelimiterBytes.length) {
                        bufferPosn++;
                        break;
                    }
                } else if (delPosn != 0) {
                    bufferPosn--;
                    delPosn = 0;
                }
            }
            int readLength = bufferPosn - startPosn;
            bytesConsumed += readLength;
            int appendLength = readLength - delPosn;
            if (appendLength > 0) {
                if (ambiguousByteCount > 0) {
                    for (int i = 0; i < ambiguousByteCount; i++, midBuffer_pos++) {
                        midBuffer[midBuffer_pos] = recordDelimiterBytes[i];
                    }
                    //appending the ambiguous characters (refer case 2.2)
                    bytesConsumed += ambiguousByteCount;
                    ambiguousByteCount = 0;
                }
                for (int i = startPosn; i < startPosn + appendLength; i++, midBuffer_pos++) {
                    midBuffer[midBuffer_pos] = buffer[i];
                }

            }
            if (bufferPosn >= bufferLength) {
                if (delPosn > 0 && delPosn < recordDelimiterBytes.length) {
                    ambiguousByteCount = delPosn;
                    bytesConsumed -= ambiguousByteCount; //to be consumed in next
                }
            }
        } while (delPosn < recordDelimiterBytes.length
                && bytesConsumed < maxPckLength);
        if (bytesConsumed > Integer.MAX_VALUE) {
            throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
        }
        return new InputRecord<>(midBuffer_pos, midBuffer);
    }
}
