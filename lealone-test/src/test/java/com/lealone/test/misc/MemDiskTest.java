/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.misc;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

public class MemDiskTest {
    public static void main(String[] args) {
        getMemInfo();
        System.out.println();
        getDiskInfo();
    }

    public static void getDiskInfo() {
        File[] disks = File.listRoots();
        // getUsableSpace和getFreeSpace返回一样的结果
        for (File file : disks) {
            System.out.print(file.getPath());
            System.out.print("  Total Space：" + toG(file.getTotalSpace()));
            System.out.print("  Used Space：" + toG(file.getTotalSpace() - file.getUsableSpace()));
            System.out.print("  Free Space：" + toG(file.getFreeSpace()));
            System.out.println();
        }
        System.out.println();
        long size = com.lealone.storage.fs.FileUtils.folderSize(new File("./target"));
        System.out.println("Target Dir Size: " + toM(size));
    }

    static String toG(long size) {
        return (size / 1024 / 1024 / 1024) + "G";
    }

    static String toM(long size) {
        return (size / 1024 / 1024) + "M";
    }

    public static void getMemInfo() {
        byte[] bytes = new byte[20 * 1024 * 1024];
        // System.out.println(bytes.length);
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }

        System.out.println("OperatingSystem：");
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        try {
            Method m = os.getClass().getMethod("getTotalPhysicalMemorySize");
            m.setAccessible(true);
            System.out.println("Total RAM：" + toM((Long) m.invoke(os)));
            m = os.getClass().getMethod("getFreePhysicalMemorySize");
            m.setAccessible(true);
            System.out.println("Free  RAM：" + toM((Long) m.invoke(os)));
            System.out.println();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        System.out.println("Runtime：");
        System.out.println("Total RAM：" + toM(Runtime.getRuntime().totalMemory()));
        System.out.println("Max   RAM：" + toM(Runtime.getRuntime().maxMemory()));
        System.out.println();

        MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        System.out.println("HeapMemory：");
        printMemoryUsage(mu);
        System.out.println();
        mu = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        System.out.println("NonHeapMemory：");
        printMemoryUsage(mu);
    }

    static void printMemoryUsage(MemoryUsage mu) {
        System.out.println("Init  RAM：" + toM(mu.getInit())); // 对应 -Xms
        System.out.println("Comm　RAM：" + toM(mu.getCommitted()));
        System.out.println("Max   RAM：" + toM(mu.getMax())); // 对应 -Xmx
        System.out.println("Used　RAM：" + toM(mu.getUsed()));
    }
}
