package uk.co.devworx.cloudfront;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

/**
 * A simple class that will be responsible for reading the files from a directory
 * containing cloudfront files.
 */
public class CloudfrontReader implements Spliterator<CloudfrontLog>, AutoCloseable
{
	private static final Logger logger = LogManager.getLogger(CloudfrontReader.class);

	private final Path rootDirectory;
	private final List<Path> childrenFiles;

	private final AtomicInteger currentFileIndex;

	private final AtomicReference<BufferedReader> currentReader;
	private final AtomicReference<Path> currentFile;
	private final AtomicInteger currentLine;

	private final AtomicReference<String> nextLine;

	public CloudfrontReader(Path rootDirectory)
	{
		this.rootDirectory = rootDirectory;
		logger.info("Instantiated with " + rootDirectory);

		if(Files.exists(rootDirectory) == false)
		{
			throw new RuntimeException("Could not find the directory : " + rootDirectory);
		}

		currentFileIndex = new AtomicInteger(0);

		try(Stream<Path> allSubPaths = Files.list(rootDirectory))
		{
			childrenFiles = allSubPaths.filter(fl -> fl.getFileName().toString().endsWith(".gz"))
									   .sorted(Comparator.comparing((p1) -> p1.getFileName().toString()))
									   .collect(Collectors.toList());
		}
		catch(IOException e)
		{
			String msg = "Unable to read the files " + rootDirectory + " - encountered : " + e;
			logger.error(msg, e);
			throw new RuntimeException(msg, e);
		}

		logger.info("Total Files to Parse " + childrenFiles.size());
		currentReader = new AtomicReference<>();
		currentFile = new AtomicReference<>();
		currentLine = new AtomicInteger();
		nextLine = new AtomicReference<>();

		//Read the first line.
		try
		{
			String nl = null;
			do
			{
				Optional<BufferedReader> bufrOpt = getBufferedReader();
				if(bufrOpt.isPresent() == false)
				{
					nl = null;
					break;
				}
				nl = bufrOpt.get().readLine();
				currentLine.incrementAndGet();
				if(nl == null)
				{
					closeReaderAndMoveOn();
				}
			}
			while(nl == null);
			nextLine.set(nl);
		}
		catch(Exception e)
		{
			String msg = "Unable to read the file : " + currentFile.get() + " - at line : " + currentLine.get() + " - Exception was : " + e;
			logger.error(msg, e);
			throw new RuntimeException(msg, e);
		}
	}

	public Stream<CloudfrontLog> stream()
	{
		return StreamSupport.stream(this, false);
	}

	@Override public boolean tryAdvance(Consumer<? super CloudfrontLog> action)
	{
		try
		{
			String nl = nextLine.get();
			if(nl == null) return false;

			CloudfrontLog cloudfrontLog = CloudfrontLog.parseRecord(nl);
			action.accept(cloudfrontLog);

			currentLine.incrementAndGet();

			nl = null;
			do
			{
				Optional<BufferedReader> bufrOpt = getBufferedReader();
				if(bufrOpt.isPresent() == false)
				{
					return false;
				}
				nl = bufrOpt.get().readLine();
				currentLine.incrementAndGet();
				if(nl == null)
				{
					closeReaderAndMoveOn();
				}
			}
			while(nl == null);
			nextLine.set(nl);

			return true;
		}
		catch(Exception e)
		{
			String msg = "Unable to read the file : " + currentFile.get() + " - at line : " + currentLine.get() + " - Exception was : " + e;
			logger.error(msg, e);
			throw new RuntimeException(msg, e);
		}

	}

	private void closeReaderAndMoveOn() throws IOException
	{
		BufferedReader bufr = currentReader.get();
		if(bufr != null)  bufr.close();
		currentReader.set(null);
	}

	private Optional<BufferedReader> getBufferedReader() throws IOException
	{
		BufferedReader bufr = currentReader.get();
		if(bufr != null) return Optional.of(bufr);

		int index = currentFileIndex.getAndIncrement();
		if(index >= childrenFiles.size())
		{
			return Optional.empty();
		}

		final Path nextPath = childrenFiles.get(index);
		InputStream ins = Files.newInputStream(nextPath);
		GZIPInputStream gzins = new GZIPInputStream(ins);
		InputStreamReader insReader = new InputStreamReader(gzins);
		BufferedReader bufReader = new BufferedReader(insReader);
		currentReader.set(bufReader);
		currentFile.set(nextPath);

		logger.info("Reading Next File : " + nextPath.getFileName() + " - index : " + (index) + " out of a total of " + childrenFiles.size());
		logger.debug("Skipping the first two lines : " + bufReader.readLine() + " | " + bufReader.readLine() );
		currentLine.set(2);
		return Optional.of(bufReader);
	}

	@Override public Spliterator<CloudfrontLog> trySplit()
	{
		return null;
	}

	@Override public long estimateSize()
	{
		return Long.MAX_VALUE;
	}

	@Override public int characteristics()
	{
		return ORDERED | SIZED | IMMUTABLE | NONNULL;
	}

	@Override public void close() throws Exception
	{
		BufferedReader bufferedReader = currentReader.get();
		if(bufferedReader != null) bufferedReader.close();
	}
}
