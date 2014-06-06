package com.sdata.apps.amazon;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by dejun on 04/06/14.
 */
public class BlockedTaskQueue<E> extends ArrayBlockingQueue<E> {
    public BlockedTaskQueue(int capacity) {
        super(capacity);
    }

    @Override
    public boolean offer(E e) {
        try {
            super.put(e);
        } catch (InterruptedException e1) {
            return false;
        }
        return true;
    }
}
