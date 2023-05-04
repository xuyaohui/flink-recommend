package flink.utils;


import flink.domin.LogEntity;


public class LogToEntity {

    public static LogEntity getLog(String s){
        System.out.println(s);
        String[] values = s.split(",");
        if (values.length < 2) {
            System.out.println("Message is not correct");
            return null;
        }
        LogEntity log = new LogEntity();
        log.setUserId(values[0]);
        log.setProductId(values[1]);

        log.setTime(Long.parseLong(values[2]));
        log.setAction(values[3]);

        return log;
    }
}
