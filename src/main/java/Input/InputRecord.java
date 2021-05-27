package Input;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/11/4 18:02
 */

/**
 * @program: BG_DAQ_FrameWork
 * @description:
 * @author: WuYe
 * @create: 2020-11-04 18:02
 **/
public class InputRecord<k,v> {
    private k key;
    private v value;

    public InputRecord(k key, v value) {
        this.key = key;
        this.value = value;
    }

    public k getKey() {
        return key;
    }

    public void setKey(k key) {
        this.key = key;
    }

    public v getValue() {
        return value;
    }

    public void setValue(v value) {
        this.value = value;
    }
}
