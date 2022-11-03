package com.example.snowflake;

import com.example.core.TableRef;
import com.example.snowflake.util.DBUtility;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SnowflakeImporterSpec {
    @Test
    public void testSelectRows() {
        String table = "MEMBERS";
        SnowflakeImporter importer = new SnowflakeImporter(null, null, null, null);

        SnowflakeTableInfo tableInfo = new SnowflakeTableInfo(new TableRef("PUBLIC", table), null, null, -1, null, SnowflakeChangeType.CHANGE_DATA_CAPTURE);
        SnowflakeDriver snowflakeDriver = new SnowflakeDriver();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = snowflakeDriver.getConnection();
            ps = importer.selectRows(con, tableInfo);
            rs = ps.executeQuery();

            int count = 0;
            while (rs.next()) {
                System.out.println(rs.getString("NAME"));
                count++;
            }

            Assert.assertTrue(count > 0);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DBUtility.close(rs);
            DBUtility.close(ps);
            DBUtility.close(con);
        }
    }
}
