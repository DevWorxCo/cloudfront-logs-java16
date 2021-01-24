package uk.co.devworx.cloudfront;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A class as the name suggests, creates a SQL (HSQLDB) database
 * out of a number of Cloudfront file directories.
 */
public class CloudfrontHSQLDBCreator implements AutoCloseable
{
	private static final Logger logger = LogManager.getLogger(CloudfrontHSQLDBCreator.class);

	public static final int BATCH_SIZE = 1000;

	public static void main(String... args) throws Exception
	{
		if(args.length < 2)
		{
			System.err.println("You must specify 2 arguments - (1) the location of the Cloudfront files - (2) The database file location");
			System.exit(1);
		}

		Path cloudfrontFiles = Paths.get(args[0]);
		Path databaseFile = Paths.get(args[1]);

		CloudfrontHSQLDBCreator creator = new CloudfrontHSQLDBCreator(cloudfrontFiles, databaseFile);
		creator.process();
		creator.close();

	}


	private final Path cloudfrontFiles;
	private final Path databaseFile;
	private final String databaseUrl;

	private final Connection con;
	private final Statement stmt;
	private final PreparedStatement prstmt;

	private final String dropSQL;
	private final String createSQL;
	private final String prepareStatementSQL;

	private final CloudfrontReader reader;

	private CloudfrontHSQLDBCreator(Path cloudfrontFilesP, Path databaseFileP)
	{
		try
		{
			Class.forName("org.hsqldb.jdbcDriver" );
		}
		catch (Exception e)
		{
			throw new RuntimeException("Unable to instantiate the HSQLDB Driver !");
		}

		this.cloudfrontFiles = cloudfrontFilesP;
		this.databaseFile = databaseFileP;

		logger.info("The CloudFront Directory : "  + cloudfrontFiles);
		logger.info("The Database File  : "  + databaseFile);

		reader = new CloudfrontReader(cloudfrontFiles);

		databaseUrl = "jdbc:hsqldb:" + databaseFile.toFile().toURI().toString();

		logger.info("The Database URI  : "  + databaseUrl);

		dropSQL = getClasspathResource("/01-table-drop-script.sql");
		createSQL = getClasspathResource("/02-table-create-script.sql");
		prepareStatementSQL = getClasspathResource("/03-insert-prepared-statement.sql");

		try
		{
			con = DriverManager.getConnection(databaseUrl, "sa", "");
			stmt = con.createStatement();

		}
		catch(Exception e)
		{
			String msg = "Unable to connect to the database : " + e;
			logger.error(msg, e);
			throw new RuntimeException(msg, e);
		}

		try
		{

			logger.info("Executing the Drop SQL.");
			stmt.execute(dropSQL);
			logger.info("Executing the Create Table SQL.");
			stmt.execute(createSQL);
			logger.info("Database setup done.");
			con.setAutoCommit(false);

			prstmt = con.prepareStatement(prepareStatementSQL);
		}
		catch(SQLException e)
		{
			String msg = "Unable to execute SQL statement to the database : " + e;
			logger.error(msg, e);
			throw new RuntimeException(msg, e);
		}

	}

	private String getClasspathResource(String path)
	{
		InputStream ins = getClass().getResourceAsStream(path);
		if(ins == null) throw new RuntimeException("Unable to resource the classpath resource : " + path + " - maybe you have not set up the project correctly.");

		StringBuilder bldr = new StringBuilder();
		String line = null;
		try(BufferedReader insReader = new BufferedReader(new InputStreamReader(ins)))
		{
			while((line = insReader.readLine()) != null)
			{
				bldr.append(line);
				bldr.append("\n");
			}
		}
		catch(Exception e)
		{
			throw new RuntimeException("Unable to read the lines from : " + path + " - encountered : " + e, e);
		}

		return bldr.toString();
	}

	private void process()
	{
		try(Stream<CloudfrontLog> stream = reader.stream())
		{
			final AtomicInteger count = new AtomicInteger();

			stream.forEach(log ->
			{
				try
				{
					prstmt.clearParameters();
					int index = 1;
					prstmt.setDate(index++, Date.valueOf(log.date()));
					prstmt.setTime(index++, java.sql.Time.valueOf(log.time()));
					prstmt.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.of(log.date(), log.time())) );
					prstmt.setString(index++, log.edgeLocation());
					prstmt.setLong(index++, log.scBytes());
					prstmt.setString(index++, log.cIP());
					prstmt.setString(index++, log.csMethod());
					prstmt.setString(index++, log.csHost());
					prstmt.setString(index++, log.csUriStem());
					prstmt.setString(index++, log.scStatus());
					prstmt.setString(index++, log.csReferer());
					prstmt.setString(index++, log.csUserAgent());
					prstmt.setString(index++, log.csUriQuery());
					prstmt.setString(index++, log.csCookie());
					prstmt.setString(index++, log.edgeResultType().toString());
					prstmt.setString(index++, log.edgeRequestId());
					prstmt.setString(index++, log.hostHeader());
					prstmt.setString(index++, log.csProtocol());
					prstmt.setLong(index++, log.csBytes());
					prstmt.setDouble(index++, log.timeTaken());
					prstmt.setString(index++, log.xForwardedFor());
					prstmt.setString(index++, log.sslProtocol());
					prstmt.setString(index++, log.sslCipher());
					prstmt.setString(index++, log.edgeResponseResultType());
					prstmt.setString(index++, log.csProtocolVersion());
					prstmt.setString(index++, log.fleStatus());
					prstmt.setString(index++, log.fleEncryptedFields());
					prstmt.setLong(index++, log.cPort());
					prstmt.setDouble(index++, log.timeToFirstByte());
					prstmt.setString(index++, log.edgeDetailedResultType());
					prstmt.setString(index++, log.contentType());
					prstmt.setLong(index++, log.contentLength());
					prstmt.setString(index++, log.rangeStart());
					prstmt.setString(index++, log.rangeEnd());

					prstmt.addBatch();

					int currentIndex = count.incrementAndGet();
					if( currentIndex % BATCH_SIZE == 0)
					{
						logger.info("Executing the Batch - Current Count : " + currentIndex);
						prstmt.executeBatch();
						con.commit();
						logger.info("Done Executing Batch");
					}

				}
				catch(SQLException e)
				{
					String msg = "Unable to execute the SQL Batch - got the exception : " + e + " - current count : " + count.get();
					logger.error(msg, e);
					throw new RuntimeException(msg, e);
				}
			});

			try
			{
				logger.info("Executing the LAST Batch - Total Count : " + count.get());
				prstmt.executeBatch();
				con.commit();
				logger.info("Done Executing Last Batch");

			}
			catch(SQLException e)
			{
				String msg = "Unable to execute the SQL Batch - got the exception : " + e + " - current count : " + count.get();
				logger.error(msg, e);
				throw new RuntimeException(msg, e);
			}
		}

	}

	@Override public void close() throws Exception
	{
		if(con != null)
		{
			con.close();
		}
	}
}
