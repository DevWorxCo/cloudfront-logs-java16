package uk.co.devworx.cloudfront;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Scanner;

/**
 * Represents a record from the Cloudfront file : https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html
 *
 */
public record CloudfrontLog(LocalDate date,
							LocalTime time,
							String edgeLocation,
							long scBytes,
							String cIP,
							String csMethod,
							String csHost,
							String csUriStem,
							String scStatus,
							String csReferer,
							String csUserAgent,
							String csUriQuery,
							String csCookie,
							XEdgeResultType edgeResultType,
							String edgeRequestId,
							String hostHeader,
							String csProtocol,
							long csBytes,
							double timeTaken,
							String xForwardedFor,
							String sslProtocol,
							String sslCipher,
							String edgeResponseResultType,
							String csProtocolVersion,
							String fleStatus,
							String fleEncryptedFields,
							long cPort,
							double timeToFirstByte,
							String edgeDetailedResultType,
							String contentType,
							long contentLength,
							String rangeStart,
							String rangeEnd
							)
{
	private static final Logger logger = LogManager.getLogger(CloudfrontLog.class);

	/**
	 * Returns a record instance from a cloud front string.
	 *
	 * @param cloudfrontLogLine
	 * @return
	 */
	public static CloudfrontLog parseRecord(String cloudfrontLogLine)
	{
		try(final Scanner scanner = new Scanner(cloudfrontLogLine).useDelimiter("\t"))
		{
			LocalDate date = LocalDate.parse(scanner.next());
			LocalTime time = LocalTime.parse(scanner.next());
			String edgeLocation = scanner.next();
			long scBytes = scanner.nextLong();
			String cIP = scanner.next();
			String csMethod = scanner.next();
			String csHost = scanner.next();
			String csUriStem = scanner.next();
			String scStatus = scanner.next();
			String csReferer = scanner.next();
			String csUserAgent = scanner.next();
			String csUriQuery = scanner.next();
			String csCookie = scanner.next();
			XEdgeResultType edgeResultType = XEdgeResultType.valueOf(scanner.next());
			String edgeRequestId = scanner.next();
			String hostHeader = scanner.next();
			String csProtocol = scanner.next();
			long csBytes = scanner.nextLong();
			double timeTaken = scanner.nextDouble();
			String xForwardedFor = scanner.next();
			String sslProtocol = scanner.next();
			String sslCipher = scanner.next();
			String edgeResponseResultType = scanner.next();
			String csProtocolVersion = scanner.next();
			String fleStatus = scanner.next();
			String fleEncryptedFields = scanner.next();

			long cPort = scanner.hasNext() ? scanner.nextLong() : -1;
			double timeToFirstByte = scanner.hasNext() ? scanner.nextDouble() : -1;
			String edgeDetailedResultType = scanner.hasNext() ? scanner.next() : "-";
			String contentType = scanner.hasNext() ? scanner.next() : "-";
			long contentLength = -1;
			if(scanner.hasNext() == true)
			{
				try
				{
					contentLength = Long.parseLong(scanner.next());
				}
				catch(Exception e)
				{
					logger.debug("Could not parse - " + e);
				}
			}

			String rangeState = scanner.hasNext() ? scanner.next() : "-";
			String rangeEnd = scanner.hasNext() ? scanner.next() : "-";

			return new CloudfrontLog(
					date,
					time,
					edgeLocation,
			scBytes,
			cIP,
			csMethod,
			csHost,
			csUriStem,
			scStatus,
			csReferer,
			csUserAgent,
			csUriQuery,
			csCookie,
			edgeResultType,
			edgeRequestId,
			hostHeader,
			csProtocol,
			 csBytes,
			 timeTaken,
			xForwardedFor,
			sslProtocol,
			sslCipher,
			edgeResponseResultType,
			csProtocolVersion,
			fleStatus,
			fleEncryptedFields,
			 cPort,
			 timeToFirstByte,
			edgeDetailedResultType,
			contentType,
			 contentLength,
			 rangeState,
					rangeEnd
			);





		}

	}

}
