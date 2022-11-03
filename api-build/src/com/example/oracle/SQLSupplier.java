package com.example.oracle;

import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 4:16 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
@FunctionalInterface
public interface SQLSupplier<T> {
    T get() throws SQLException;
}
