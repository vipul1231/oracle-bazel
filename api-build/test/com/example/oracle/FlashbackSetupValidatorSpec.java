package com.example.oracle;

import com.example.oracle.flashback.FlashbackSetupValidator;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.example.oracle.AbstractOracleTestSpec.mockConnection;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/15/2021<br/>
 * Time: 7:44 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class FlashbackSetupValidatorSpec {

    private FlashbackSetupValidator flashbackSetupValidator;

    @Before
    public void beforeFlashbackSetupValidatorSpec() {
        flashbackSetupValidator = mock(FlashbackSetupValidator.class);
    }

    @Test
    public void flashbackDataArchive_Disabled() throws SQLException {
        // Arrange
        ResultSet mockResultSet = mock(ResultSet.class);
        doReturn(false).when(mockResultSet).next();

        // Act
        boolean isEnabled =
                flashbackSetupValidator.flashbackDataArchiveEnabled(mockConnection(mockResultSet));

        // Assert
        assertFalse(isEnabled);
    }

    @Test
    public void flashbackDataArchive_Enabled() throws SQLException {
        // Arrange
        ResultSet mockResultSet = mock(ResultSet.class);
        doReturn(true).when(mockResultSet).next();

        // Act
        boolean isEnabled =
                flashbackSetupValidator.flashbackDataArchiveEnabled(mockConnection(mockResultSet));

        // Assert
        assertTrue(isEnabled);
    }

    @Test
    public void flashbackDataArchive_CannotLookUpFlashbackArchive() throws SQLException {

        // Arrange
        SQLException mockSQLException = mock(SQLException.class);
        doReturn("some other error").when(mockSQLException).getMessage();

        Statement mockStatement = mock(Statement.class);
        doThrow(mockSQLException).when(mockStatement).executeQuery(anyString());

        // Act
//        RuntimeException exception =
//                assertThrows(
//                        RuntimeException.class,
//                        () ->
//                                flashbackSetupValidator.flashbackDataArchiveEnabled(
//                                        mockConnection(mockStatement)));
//        assertTrue(exception.getMessage().contains("Could not look up flashback archive"));
    }
}