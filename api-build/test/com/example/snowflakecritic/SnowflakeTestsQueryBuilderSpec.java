package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SnowflakeTestsQueryBuilderSpec extends TestBase{


	@Test
	public void testQueryUtilsEscapeCharacter() {
		assertEquals("\"COLUMN\"", SnowflakeSQLUtils.escape("COLUMN"));
	}

	@Test
	public void testQueryUtilsSelectStatement() {
		Set<SnowflakeColumnInfo> snowflakeColumnInfoSet = new HashSet<>();
		SnowflakeColumnInfo columnInfo1 = new SnowflakeColumnInfo(null,
				"C1",
				0,
				SnowflakeType.DATE,
				DataType.LocalDate,
				null,
				null,
				null,
				null,
				false,
				false,
				false,
				null,
				false,
				false);
		SnowflakeColumnInfo columnInfo = new SnowflakeColumnInfo(null,
				"C2",
				0,
				SnowflakeType.TEXT,
				DataType.String,
				null,
				null,
				null,
				null,
				false,
				false,
				false,
				null,
				false,
				false);

		snowflakeColumnInfoSet.add(columnInfo);
		snowflakeColumnInfoSet.add(columnInfo1);

		String output = SnowflakeSQLUtils.select(snowflakeColumnInfoSet, "TEST_TABLE");
		assertEquals("SELECT \"C1\",\"C2\" FROM TEST_TABLE", output);
	}

	@Test
	public void testQueryUtilsForUpperCaseAndQuote() {
		TableRef tableRef = new TableRef("runner","skyline");
		assertEquals("\"RUNNER\".\"SKYLINE\"", SnowflakeSQLUtils.quote(tableRef));
	}

	@Test
	public void testQueryUtilsForWhereIn() {
		List<String> columnNames = new ArrayList<>();
		columnNames.add("COL1");
		columnNames.add("COL2");

		List<List<Object>> info = new ArrayList<>();

		List<Object> test1 = new ArrayList<>();
		test1.add("T1");
		test1.add("T2");
		List<Object> test2 = new ArrayList<>();
		test2.add("T3");
		test2.add("T4");

		info.add(test1);
		info.add(test2);
		assertEquals("\"COL1\" IN (\"T1\", \"T3\") AND \"COL2\" IN (\"T4\", \"T2\")", SnowflakeSQLUtils.whereIn(columnNames, info));
	}
}
