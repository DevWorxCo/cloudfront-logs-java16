package uk.co.devworx.cloudfront;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Small test case for CloudfrontHSQLDBCreator
 */
public class CloudfrontHSQLDBCreatorTest
{
	private static final Logger logger = LogManager.getLogger(CloudfrontHSQLDBCreatorTest.class);

	@Test
	public void testDatabaseCreation() throws Exception
	{
		String pathStr = "target/test-database/clflogs";
		CloudfrontHSQLDBCreator.main("src/test/resources/test-data/devworx.co.uk", pathStr );

		Path databaseFile = Paths.get(pathStr);

		String databaseUrl = "jdbc:hsqldb:" + databaseFile.toFile().toURI().toString();

		logger.info("Getting connection to : " + databaseUrl);

		Connection con = DriverManager.getConnection(databaseUrl, "sa", "");
		Statement stmt = con.createStatement();

		ResultSet rs = stmt.executeQuery("SELECT * FROM CLOUDFRONT_LOGS");
		int count = 0;
		while(rs.next())
		{
			count++;
		}

		rs.close();
		stmt.close();
		con.close();

		Assertions.assertEquals(3, count);

	}

}
