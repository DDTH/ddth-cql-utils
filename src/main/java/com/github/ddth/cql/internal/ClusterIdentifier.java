package com.github.ddth.cql.internal;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * For internal use.
 * 
 * @author Thanh Ba Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class ClusterIdentifier {
    public String hostsAndPorts, username, password;

    public ClusterIdentifier(String hostsAndPorts, String username, String password) {
        this.hostsAndPorts = hostsAndPorts;
        this.username = username;
        this.password = password;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(hostsAndPorts).append(username).append(password);
        return hcb.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ClusterIdentifier) {
            ClusterIdentifier other = (ClusterIdentifier) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(hostsAndPorts, other.hostsAndPorts).append(username, other.username)
                    .append(password, other.password);
            return eq.isEquals();
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("hostsAndPorts", hostsAndPorts).append("username", username).append("password",
                "*");
        return tsb.toString();
    }
}
