package com.pivotal.stream;

import com.pivotal.stream.s3.source.MinioS3SourceProperties;
import io.minio.MinioClient;
import io.minio.ObjectStat;
import io.minio.messages.Item;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.integration.endpoint.SourcePollingChannelAdapter;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
@TestPropertySource(properties = {
		"cloud.aws.stack.auto=false",
		"accessKey=" + MinioS3ApplicationTests.AWS_ACCESS_KEY,
		"secretKey=" + MinioS3ApplicationTests.AWS_SECRET_KEY,
		"endPoint=" + MinioS3ApplicationTests.S3_ENDOINT,
		"cloud.aws.region.static=" + MinioS3ApplicationTests.AWS_REGION,
		"trigger.initialDelay=1",
		"s3.remoteDir=" + MinioS3ApplicationTests.S3_BUCKET })
@SpringBootTest
public class MinioS3ApplicationTests {
	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	protected static final String AWS_ACCESS_KEY = "test.accessKey";

	protected static final String AWS_SECRET_KEY = "test.secretKey";

	protected static final String AWS_REGION = "us-east-1";

	protected static final String S3_ENDOINT = "http://192.168.0.106:9000";

	protected static final String S3_BUCKET = "S3_BUCKET";

	private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

	protected static List<ObjectStat> S3_OBJECTS;

	@Autowired
	protected SourcePollingChannelAdapter s3ChannelAdapter;

	@Autowired
	protected MinioClient minioClient;

	@Autowired
	protected Source channels;

	@Autowired
	protected MinioS3SourceProperties config;

	@Test
	public void contextLoads() {
	}

	@BeforeClass
	public static void setup() throws Exception {
		File remoteFolder = TEMPORARY_FOLDER.newFolder("remote");

		File aFile = new File(remoteFolder, "1.test");
		FileCopyUtils.copy("Hello".getBytes(), aFile);
		File bFile = new File(remoteFolder, "2.test");
		FileCopyUtils.copy("Bye".getBytes(), bFile);
		File otherFile = new File(remoteFolder, "otherFile");
		FileCopyUtils.copy("Other\nOther2".getBytes(), otherFile);

		S3_OBJECTS = new ArrayList<>();

		for (File file : remoteFolder.listFiles()) {
			Calendar expectedDate = Calendar.getInstance();
			expectedDate.clear();
			expectedDate.setTimeZone(TimeZone.getTimeZone("GMT"));
			expectedDate.set(2015, Calendar.MAY, 4, 7, 58, 51);
			ObjectStat expectedStatInfo = new ObjectStat(S3_BUCKET, file.getName(),
					expectedDate.getTime(),
					5080,
					"a670520d9d36833b3e28d1e4b73cbe22",
					APPLICATION_OCTET_STREAM);
			S3_OBJECTS.add(expectedStatInfo);
		}

		String localFolder = TEMPORARY_FOLDER.newFolder("local").getAbsolutePath();

		System.setProperty("s3.localDir", localFolder);
	}

	@SpringBootApplication
	public static class S3SourceApplication {

	}
}
