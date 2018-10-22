package com.github.ddth.cql.utils;

/**
 * Thrown to indicate that number of async jobs has exceeded the maximum value.
 * 
 * @author Thanh Ba Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class ExceedMaxAsyncJobsException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private int maxAsyncJobs;

    public ExceedMaxAsyncJobsException(int maxAsyncJobs) {
        super("Max async-jobs exceeds " + maxAsyncJobs);
        this.maxAsyncJobs = maxAsyncJobs;
    }

    public ExceedMaxAsyncJobsException(int maxAsyncJobs, String message) {
        super(message);
        this.maxAsyncJobs = maxAsyncJobs;
    }

    public int getMaxAsyncJobs() {
        return maxAsyncJobs;
    }
}
