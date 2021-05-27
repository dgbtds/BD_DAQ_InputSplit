package Util;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/10/29 19:43
 */

import java.io.*;

/**
 * @program: BG_DAQ_FrameWork
 *
 * @description:
 *
 * @author: WuYe
 *
 * @create: 2020-10-29 19:43
 **/
public class FileUtil {
    public static String ReadFile(File file){
        BufferedReader reader = null;
        StringBuilder laststr = new StringBuilder();
        try{
            FileInputStream fileInputStream = new FileInputStream(file);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            reader = new BufferedReader(inputStreamReader);
            String tempString = null;
            while((tempString = reader.readLine()) != null){
                laststr.append(tempString);
            }
            reader.close();
        }catch(IOException e){
            e.printStackTrace();
        }finally{
            if(reader != null){
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return laststr.toString();
    }
}
