package com.example.snowflakecritic.scripts;

import com.example.snowflakecritic.SnowflakeSource;

public class SnowflakeEndToEnd extends BaseScriptRunner{

	public SnowflakeEndToEnd(SnowflakeSource source, JsonFileHelper fileHelper) {
		super(source, fileHelper);
	}

	@Override
	public void run() {

		try {
			SnowflakeE2ERunner systemInfo = new SnowflakeE2ERunner(source);
			source.execute( connection -> {
				System.out.println("Checking system info for table creation");
				systemInfo.checkSnowFlakeSystemInfo();
				systemInfo.createTable();
				System.out.println("Created table successfully");
				systemInfo.insertIntoTable();
				System.out.println("Inserted into table successfully");
				String result = systemInfo.readFromTable();
				System.out.println("Data: "+result);
				systemInfo.dropTable();
				System.out.println("Drop table successfully");
			});
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
