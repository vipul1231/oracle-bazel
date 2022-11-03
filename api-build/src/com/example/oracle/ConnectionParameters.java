package com.example.oracle;

import java.util.Objects;
import java.util.TimeZone;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 5:15 PM<br/>
 * To change this template use File | Settings | File Templates.
 */

/** Parameters that are collected in the wizard but stored directly in the integrations table. */
public class ConnectionParameters implements Comparable<ConnectionParameters> {
    /** Group ID of connection owner */
    public final String owner;

    /**
     * Unique name of this integration. Usually the same as the name of the schema, but in the case of multi-schema
     * integrations like Postgres or webhooks, just a name.
     */
    public final String schema;

    /** The Time Zone the connecting owner (group) is operating in */
    public final TimeZone timeZone;

    @Deprecated
    public ConnectionParameters(String owner, String schema) {
        this(owner, schema, null);
    }

    public ConnectionParameters(String owner, String schema, TimeZone timeZone) {
        this.owner = owner;
        this.schema = schema;
        this.timeZone = timeZone;
    }

    public ConnectionParameters() {
        this("owner", "schema", TimeZone.getDefault());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ConnectionParameters)) return false;

        ConnectionParameters otherParams = ((ConnectionParameters) other);
        return com.google.common.base.Objects.equal(this.owner, otherParams.owner)
                && com.google.common.base.Objects.equal(this.schema, otherParams.schema)
                && com.google.common.base.Objects.equal(this.timeZone, otherParams.timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner, schema, timeZone);
    }

    @Override
    public int compareTo(ConnectionParameters that) {
        int ownerCompare = this.owner.compareTo(that.owner);
        return ownerCompare != 0 ? ownerCompare : this.schema.compareTo(that.schema);
    }
}