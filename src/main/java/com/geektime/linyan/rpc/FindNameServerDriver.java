package com.geektime.linyan.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class FindNameServerDriver {
    public static void main(String[] args) throws IOException {
        // 创建RPC的配置
        Configuration configuration = new Configuration();
        // 构建RPC的builder对象
        RPC.Builder builder = new RPC.Builder(configuration);
        // 设置RPC Server的信息
        //设置服务器IP地址
        builder.setBindAddress("127.0.0.1");
        //设置端口号
        builder.setPort(54321);

        builder.setProtocol(FindNameService.class);
        builder.setInstance(new FindNameServiceImpl());

/*        RPC.Server server = builder.setBindAddress("localhost")
                .setPort(4893)
                .setProtocol(FindNameService.class)
                .setInstance(new FindNameServiceImpl())
                .build();*/
        // 启动RPC Server，返回一个server对象
        // 这是守护进程，main函数不会退出
        try {
            RPC.Server server = builder.build();
            server.start();
            System.out.println("服务启动成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
