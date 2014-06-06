package com.sdata.apps.amazon;

import junit.framework.TestCase;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockedTaskQueueTest extends TestCase {

    public void testOffer() throws Exception {
        final BlockedTaskQueue<String> blockedTaskQueue = new BlockedTaskQueue<>(10);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                blockedTaskQueue.poll();
                System.out.println("pop up a string");
            }
        }).start();
        for(int i=0;i<11;i++){
            String item = "String" + i;
            blockedTaskQueue.offer(item);
            System.out.println(item);
        }
    }

    public void testExecutorOffer() throws Exception {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(2, 2,
                0L, TimeUnit.MILLISECONDS,
                new BlockedTaskQueue<Runnable>(2));
        final AtomicInteger counter = new AtomicInteger(1);
        for(int i=0;i<11;i++) {
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    System.out.println("task " + counter.incrementAndGet());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}