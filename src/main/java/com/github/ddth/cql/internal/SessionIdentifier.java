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
public class SessionIdentifier extends ClusterIdentifier {
    public String keyspace;

    public SessionIdentifier(String hostsAndPorts, String username, String password, String keyspace) {
        super(hostsAndPorts, username, password);
        this.keyspace = keyspace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(keyspace).append(super.hashCode());
        return hcb.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SessionIdentifier) {
            SessionIdentifier other = (SessionIdentifier) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(keyspace, other.keyspace);
            return super.equals(obj) && eq.isEquals();
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("keyspace", keyspace).appendSuper(super.toString());
        return tsb.toString();
    }
}
