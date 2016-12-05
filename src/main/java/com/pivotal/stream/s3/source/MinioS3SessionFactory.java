package com.pivotal.stream.s3.source;

import io.minio.MinioClient;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import io.minio.messages.Item;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.file.remote.session.Session;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.remote.session.SharedSessionCapable;
import org.springframework.util.Assert;

/**
 * Created by mwright on 12/4/16.
 */
public class MinioS3SessionFactory implements SessionFactory<Item>, SharedSessionCapable {
    private static final Log logger = LogFactory.getLog(MinioS3SessionFactory.class);

    @Value("${accessKey}")
    private static String accessKey;

    @Value("${secretKey}")
    private static String secretKey;

    @Value("${endpoint}")
    private static String endPoint;

    private final MinioS3Session s3Session;

    public MinioS3SessionFactory() throws Exception {
        this(MinioS3SessionFactory.getMinioClient());
    }

//    public MinioS3SessionFactory(MinioClient minioClient) {
//        this(minioClient,null);
//    }

    public MinioS3SessionFactory(MinioClient minioClient) {
        Assert.notNull(minioClient, "'minioClient' must not be null.");
        this.minioClient = minioClient;
        this.s3Session = new MinioS3Session(minioClient);
    }

    private MinioClient minioClient;

    @Override
    public Session<Item> getSession() {
        return s3Session;
    }

    @Override
    public boolean isSharedSession() {
        return true;
    }

    @Override
    public void resetSharedSession() {

    }

    private static MinioClient getMinioClient() throws InvalidPortException, InvalidEndpointException {

            logger.info("Connecting to " + endPoint);
            MinioClient minioClient = new MinioClient(endPoint, accessKey, secretKey);

        return minioClient;
    }
}
