package com.example.snowflakecritic.scripts;

import com.example.snowflakecritic.SnowflakeSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SnowflakeE2ERunner {

	private final SnowflakeSource source;

	public SnowflakeE2ERunner(SnowflakeSource source) {
		this.source = source;
	}

	public String CREATE_TABLE = "CREATE TABLE PERSON (ID int, name character varying, age int)";

	public String DROP_TABLE = "DROP TABLE PERSON";

	public String INSERT_TABLE = "INSERT INTO PERSON values(?, ?, ?)";

	public void checkSnowFlakeSystemInfo() {
		source.execute(this::checkSystemInfo);
	}

	//TODO: check for any better way to do this ?
	public void createTable() {
		source.execute(this::executeCreateQuery);
	}

	public void dropTable() {
		source.execute(this::executeDropQuery);
	}

	public void insertIntoTable() {
		source.execute(this::executeInsertQuery);
	}

	public String readFromTable() {
		return source.execute(this::queryInsertedData);
	}

	public void executeInsertQuery(Connection connection) {
		try(PreparedStatement preparedStatement = connection.prepareStatement(INSERT_TABLE)) {
			//TODO: need improved way
			preparedStatement.setInt(1, 1);
			preparedStatement.setString(2, "ABC");
			preparedStatement.setInt(3, 45);
			preparedStatement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void executeCreateQuery(Connection connection) {
		try(PreparedStatement preparedStatement = connection.prepareStatement(CREATE_TABLE)) {
			preparedStatement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void executeDropQuery(Connection connection) {
		try(PreparedStatement preparedStatement = connection.prepareStatement(DROP_TABLE)) {
			preparedStatement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void checkSystemInfo(Connection connection) throws SQLException {
		try (PreparedStatement statement =
				     connection.prepareStatement(
						     "SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()");
		     ResultSet resultSet = statement.executeQuery()) {
			while (resultSet.next()) {
				System.out.println("Current warehouse: " + resultSet.getString(1));
				System.out.println("Current database: " + resultSet.getString(2));
				System.out.println("Current schema: " + resultSet.getString(3));
				System.out.println("Current role: " + resultSet.getString(4));
			}
		}
	}

	public String queryInsertedData(Connection connection) throws SQLException {
		return executeSqlQuery(connection, "SELECT * FROM PERSON");
	}

	private String executeSqlQuery(Connection connection, String sql)
			throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(sql);
		     ResultSet resultSet = statement.executeQuery()) {
			StringBuilder stringBuilder = new StringBuilder();
			while (resultSet.next()) {
				stringBuilder.append(resultSet.getInt(1)).append(" ")
						.append(resultSet.getString(2)).append(" ")
						.append(resultSet.getInt(3));
			}
			return stringBuilder.toString();
		} catch (RuntimeException e) {
			if (e.getCause().getClass().isAssignableFrom(SQLException.class)) {
				throw (SQLException) e.getCause();
			}
			throw e;
		}
	}
}
