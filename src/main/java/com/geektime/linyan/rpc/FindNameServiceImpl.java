package com.geektime.linyan.rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class FindNameServiceImpl implements FindNameService {
    //实现加法
    @Override
    public int add(int number1, int number2) {
        System.out.println("number1 = " + number1 + " number2 = " + number2);
        return number1 + number2;
    }
    //实现SayHi方法
    @Override
    public String sayHi(String name) {
        System.out.println("名字：" + name);
        return "Hi, " + name;
    }
    //实现findName方法
    @Override
    public String findName(long studentId) {
        if (studentId == 20210123456789L) {
            System.out.println("心心");
            return "心心";
        } else {
            System.out.println("请输入正确的学号");
            return null;
        }
/*        switch (studentId) {
            case 2021012345:
                System.out.println("心心");
                return "心心";
            default:
                System.out.println("请输入正确的学号");
                return null;
}*/
    }
    //返回版本号
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return FindNameService.versionID;
    }
    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return new ProtocolSignature();
        //return new ProtocolSignature(getProtocolVersion(protocol, clientVersion),null);
    }
}
