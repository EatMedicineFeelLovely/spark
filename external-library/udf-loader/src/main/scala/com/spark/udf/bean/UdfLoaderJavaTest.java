package com.spark.udf.bean;
import org.apache.commons.lang3.time.DateFormatUtils;
public class UdfLoaderJavaTest {
    /**
     *
     * @return
     */
    public String getCurrentDay(){
        return DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss");
    }

//    public String getCurrentDay(String format ){
//        return DateFormatUtils.format(System.currentTimeMillis(), format);
//    }

}
