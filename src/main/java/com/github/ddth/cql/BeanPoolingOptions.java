package com.github.ddth.cql;

import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;

/**
 * Wrap around {@link PoolingOptions} to provide bean-style getters/setters.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.2.5
 */
public class BeanPoolingOptions extends PoolingOptions {
    /**
     * Delegates to {@link #getCoreConnectionsPerHost(HostDistance.LOCAL)}
     * 
     * @return
     */
    public int getCoreConnectionsPerLocalHost() {
        return getCoreConnectionsPerHost(HostDistance.LOCAL);
    }

    /**
     * Delegates to
     * {@link #setCoreConnectionsPerHost(HostDistance.LOCAL, value)}
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setCoreConnectionsPerLocalHost(int value) {
        setCoreConnectionsPerHost(HostDistance.LOCAL, value);
        return this;
    }

    /**
     * Delegates to {@link #getCoreConnectionsPerHost(HostDistance.IGNORED)}
     * 
     * @return
     */
    public int getCoreConnectionsPerIgnoredHost() {
        return getCoreConnectionsPerHost(HostDistance.IGNORED);
    }

    /**
     * Delegates to
     * {@link #setCoreConnectionsPerHost(HostDistance.IGNORED, value)}
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setCoreConnectionsPerIgnoredHost(int value) {
        setCoreConnectionsPerHost(HostDistance.IGNORED, value);
        return this;
    }

    /**
     * Delegates to {@link #getCoreConnectionsPerHost(HostDistance.REMOTE)}
     * 
     * @return
     */
    public int getCoreConnectionsPerRemoteHost() {
        return getCoreConnectionsPerHost(HostDistance.REMOTE);
    }

    /**
     * Delegates to
     * {@link #setCoreConnectionsPerHost(HostDistance.REMOTE, value)}
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setCoreConnectionsPerRemoteHost(int value) {
        setCoreConnectionsPerHost(HostDistance.REMOTE, value);
        return this;
    }

    /**
     * Delegates to {@link #getMaxConnectionsPerHost(HostDistance.LOCAL)}
     * 
     * @return
     */
    public int getMaxConnectionsPerLocalHost() {
        return getMaxConnectionsPerHost(HostDistance.LOCAL);
    }

    /**
     * Delegates to {@link #setMaxConnectionsPerHost(HostDistance.LOCAL, value)}
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setMaxConnectionsPerLocalHost(int value) {
        setMaxConnectionsPerHost(HostDistance.LOCAL, value);
        return this;
    }

    /**
     * Delegates to {@link #getMaxConnectionsPerHost(HostDistance.IGNORED)}
     * 
     * @return
     */
    public int getMaxConnectionsPerIgnoredHost() {
        return getCoreConnectionsPerHost(HostDistance.IGNORED);
    }

    /**
     * Delegates to
     * {@link #setMaxConnectionsPerHost(HostDistance.IGNORED, value)}
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setMaxConnectionsPerIgnoredHost(int value) {
        setMaxConnectionsPerHost(HostDistance.IGNORED, value);
        return this;
    }

    /**
     * Delegates to {@link #getMaxConnectionsPerHost(HostDistance.REMOTE)}
     * 
     * @return
     */
    public int getMaxConnectionsPerRemoteHost() {
        return getMaxConnectionsPerHost(HostDistance.REMOTE);
    }

    /**
     * Delegates to
     * {@link #setMaxConnectionsPerHost(HostDistance.REMOTE, value)}
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setMaxConnectionsPerRemoteHost(int value) {
        setMaxConnectionsPerHost(HostDistance.REMOTE, value);
        return this;
    }

    /**
     * Delegates to {@link #getMaxRequestsPerConnection(HostDistance.LOCAL)}.
     * 
     * @return
     */
    public int getMaxRequestsPerLocalConnection() {
        return getMaxRequestsPerConnection(HostDistance.LOCAL);
    }

    /**
     * Delegates to
     * {@link #setMaxRequestsPerConnection(HostDistance.LOCAL, value)}.
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setMaxRequestsPerLocalConnection(int value) {
        setMaxRequestsPerConnection(HostDistance.LOCAL, value);
        return this;
    }

    /**
     * Delegates to {@link #getMaxRequestsPerConnection(HostDistance.IGNORED)}.
     * 
     * @return
     */
    public int getMaxRequestsPerIgnoredConnection() {
        return getMaxRequestsPerConnection(HostDistance.IGNORED);
    }

    /**
     * Delegates to
     * {@link #setMaxRequestsPerConnection(HostDistance.IGNORED, value)}.
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setMaxRequestsPerIgnoredConnection(int value) {
        setMaxRequestsPerConnection(HostDistance.IGNORED, value);
        return this;
    }

    /**
     * Delegates to {@link #getMaxRequestsPerConnection(HostDistance.REMOTE)}.
     * 
     * @return
     */
    public int getMaxRequestsPerRemoteConnection() {
        return getMaxRequestsPerConnection(HostDistance.REMOTE);
    }

    /**
     * Delegates to
     * {@link #setMaxRequestsPerConnection(HostDistance.REMOTE, value)}.
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setMaxRequestsPerRemoteConnection(int value) {
        setMaxRequestsPerConnection(HostDistance.REMOTE, value);
        return this;
    }

    /**
     * Delegates to {@link #getNewConnectionThreshold(HostDistance.LOCAL)}.
     * 
     * @return
     */
    public int getNewConnectionThresholdLocal() {
        return getNewConnectionThreshold(HostDistance.LOCAL);
    }

    /**
     * Delegates to
     * {@link #setNewConnectionThreshold(HostDistance.LOCAL, value)}
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setNewConnectionThresholdLocal(int value) {
        setNewConnectionThreshold(HostDistance.LOCAL, value);
        return this;
    }

    /**
     * Delegates to {@link #getNewConnectionThreshold(HostDistance.IGNORED)}.
     * 
     * @return
     */
    public int getNewConnectionThresholdIgnored() {
        return getNewConnectionThreshold(HostDistance.IGNORED);
    }

    /**
     * Delegates to
     * {@link #setNewConnectionThreshold(HostDistance.IGNORED, value)}
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setNewConnectionThresholdIgnored(int value) {
        setNewConnectionThreshold(HostDistance.IGNORED, value);
        return this;
    }

    /**
     * Delegates to {@link #getNewConnectionThreshold(HostDistance.REMOTE)}.
     * 
     * @return
     */
    public int getNewConnectionThresholdRemote() {
        return getNewConnectionThreshold(HostDistance.REMOTE);
    }

    /**
     * Delegates to
     * {@link #setNewConnectionThreshold(HostDistance.REMOTE, value)}
     * 
     * @param value
     * @return
     */
    public BeanPoolingOptions setNewConnectionThresholdRemote(int value) {
        setNewConnectionThreshold(HostDistance.REMOTE, value);
        return this;
    }
}
