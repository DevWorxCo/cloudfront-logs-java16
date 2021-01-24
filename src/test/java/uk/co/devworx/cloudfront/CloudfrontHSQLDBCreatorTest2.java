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
public class CloudfrontHSQLDBCreatorTest2
{
	private static final Logger logger = LogManager.getLogger(CloudfrontHSQLDBCreatorTest2.class);

	@Test
	public void testDatabaseCreation() throws Exception
	{
		CloudfrontHSQLDBCreator.main("/mnt/JS/git/DevWorxCo/access-logs/devworx.co.uk", "/tmp/database-test1" );

	}

}
