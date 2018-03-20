package org.apache.kafka.streams.mapr;

import org.apache.kafka.streams.errors.mapr.InternalStreamNotExistException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Utils {
    public static void internalStreamExistanceCheck(String internalStream){
        String configurationClassName = "org.apache.hadoop.conf.Configuration";
        String streamsClassName = "com.mapr.streams.Streams";
        String adminClassName = "com.mapr.streams.Admin";
        String newAdminMethodName = "newAdmin";
        String streamExistsMethodName = "streamExists";
        try {
            Class<?> configurationClass = Class.forName(configurationClassName);
            Class<?> streamsClass = Class.forName(streamsClassName);
            Class<?> adminClass = Class.forName(adminClassName);
            Method newAdminMethod = streamsClass.getMethod(newAdminMethodName,configurationClass);
            Method streamExistsMethod = adminClass.getMethod(streamExistsMethodName, String.class);
            Object configuration = configurationClass.newInstance();
            Object admin = newAdminMethod.invoke(null,configuration);
            Boolean res = (Boolean) streamExistsMethod.invoke(admin, internalStream);
            if(!res){
                throw new InternalStreamNotExistException(internalStream);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("Error occurred while invoking Class.forName().\n==> %s.", e.getMessage()), e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(String.format("Error occurred while invoking Class.getMethod().\n==> %s.", e.getMessage()), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format("Error occurred while invoking Class.newInstance().\n==> %s.", e.getMessage()), e);
        } catch (InstantiationException e) {
            throw new RuntimeException(String.format("Error occurred while invoking Class.newInstance().\n==> %s.", e.getMessage()), e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(String.format("Error occurred while invoking Method.invoke().\n==> %s.", e.getMessage()), e);
        }
    }
}
