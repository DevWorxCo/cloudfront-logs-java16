package uk.co.devworx.cloudfront;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CloudfrontReaderTest
{
	private static final Logger logger = LogManager.getLogger(CloudfrontReaderTest.class);

	@Test
	public void testCloudfrontReader() throws Exception
	{
		final CloudfrontReader reader = new CloudfrontReader(Paths.get("src/test/resources/test-data/devworx.co.uk"));
		final Stream<CloudfrontLog> cloudfrontStream = reader.stream();
		final List<CloudfrontLog> allLogs = cloudfrontStream.collect(Collectors.toList());
		Assertions.assertEquals(3, allLogs.size());

		allLogs.forEach(log ->
		{
			logger.info(log);
		});

		CloudfrontLog log = allLogs.get(0);

	}

}
