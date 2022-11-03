package com.example.db;

import javax.sql.DataSource;
import java.util.concurrent.Callable;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 8/13/2021<br/>
 * Time: 3:20 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public abstract class Retrier {
    protected abstract Exception handleException(Exception exception, int attempt) throws Exception;

    protected abstract void waitForRetry(Exception exception, int attempt) throws InterruptedException;

    public abstract DataSource get(Callable<DataSource> dataSourceInit, int i);
}