package Util;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/10/30 20:45
 */

/**
 * @program: BG_DAQ_FrameWork
 * @description:
 * @author: WuYe
 * @create: 2020-10-30 20:45
 **/
public class IntByteConvert {
    //byte 与 int 的相互转换
    public static byte intToByte(int x) {
        return (byte) x;
    }

    public static int byteToInt(byte b) {
        //Java 总是把 byte 当做有符处理；我们可以通过将其和 0xFF 进行二进制与得到它的无符值
        return b & 0xFF;

    }

    public static int byteArrayToInt(byte[] b) {
        return b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[]{
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public static int byteArrayToInt(byte[] b, int start) {
        if (start + 3 >= b.length) {
            return -1;
        }
        return b[3 + start] & 0xFF |
                (b[2 + start] & 0xFF) << 8 |
                (b[1 + start] & 0xFF) << 16 |
                (b[start] & 0xFF) << 24;
    }

    public static void intToByteArray(byte[] b, int start, int a) {
        if (start + 3 >= b.length) {
            return;
        }
        b[start] = (byte) ((a >> 24) & 0xFF);
        b[start + 1] = (byte) ((a >> 16) & 0xFF);
        b[start + 2] = (byte) ((a >> 8) & 0xFF);
        b[start + 3] = (byte) (a & 0xFF);
    }
}
