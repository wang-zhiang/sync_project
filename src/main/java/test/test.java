package test;

import java.util.HashMap;
import java.util.Map;

public class test {
    public static void main(String[] args) throws InterruptedException {
        Map<Integer, String> unsafeMap = new HashMap<>();

        // 两个线程同时操作HashMap
        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                unsafeMap.put(i, "Thread1-" + i);
            }
        });

        Thread t2 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                unsafeMap.put(i, "Thread2-" + i);
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        // 结果可能：数据丢失、死循环、程序崩溃等
        System.out.println("HashMap大小: " + unsafeMap.size());
        // 可能不是1000，甚至可能程序异常退出
    }
}