package com.github.ddth.cql.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A callback for processing result of CQL execution asynchronously.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.0.0
 */
public interface Callback<T> {
    /**
     * Invoked when computation is successful.
     * 
     * @param result
     */
    void onSuccess(T result);

    /**
     * Invoked when computation fails or is canceled.
     *
     * <p>
     * If the future's {@link Future#get() get} method throws an {@link ExecutionException}, then
     * the cause is passed to this method. Any other thrown object is passed unaltered.
     * </p>
     */
    void onFailure(Throwable t);
}
