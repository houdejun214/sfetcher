package com.sdata.common;

/**
 * Created by dejun on 10/06/14.
 */
public class BlockedException extends RuntimeException {

    public BlockedException() {
    }

    public BlockedException(String message) {
        super(message);
    }
}