package com.geektime.linyan.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class FindNameClientDriver {
    public static void main(String[] args) throws IOException {
        // 构建InetSocketAddress对象
        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 54321);
        // 通过RPC.getProxy方法获得代理对象
        FindNameService findNameServiceProxy = RPC.getProxy(FindNameService.class, FindNameService.versionID, address, new Configuration());
        int result_1 = findNameServiceProxy.add(1, 2);
        System.out.println(result_1);
        String result_2 = findNameServiceProxy.sayHi("心心");
        System.out.println(result_2);
        String result_3 = findNameServiceProxy.findName(20210123456789L);
        System.out.println(result_3);
    }
}
