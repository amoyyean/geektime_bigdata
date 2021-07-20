package com.geektime.linyan.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface FindNameService extends VersionedProtocol {
    long versionID = 1L;
    int add(int number1, int number2);
    String sayHi(String name);
    String findName(String studentId);
    }
