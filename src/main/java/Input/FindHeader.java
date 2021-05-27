package Input;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/11/16 22:16
 */

/**
 * @program: BD_DAQ_InputSplit
 * @description:
 * @author: WuYe
 * @create: 2020-11-16 22:16
 **/
public class FindHeader {
    public static void main(String[] args) {
        //find delemiter
        int bufferPosn = 0;
        int delPosn=0;
        byte[] buffer = {1, 2, 3, 4, 1,2,5, 6, 7, 8, 9};
        int startPosn = bufferPosn; // Start from previous end position
        int bufferLength = buffer.length;
        byte[] recordDelimiterBytes={1,2,5,6};
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
    }
}
