package com.pivotal.stream.s3.source;

import io.minio.MinioClient;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by mwright on 12/4/16.
 */
@Configuration
//@ConditionalOnMissingAmazonClient(MinioClient.class)
public class MinioS3Configuration {
    @Value("${s3.accessKey}")
    private String accessKey;

    @Value("${s3.secretKey}")
    private String secretKey;

    @Value("${s3.endpoint}")
    private String endPoint;

    @Value("${s3.connectTimeout:10000}")
    private Integer connectTimeout;

    @Value("${s3.writeTimeout:10000}")
    private Integer writeTimeout;

    @Value("${s3.readTimeout:10000}")
    private Integer readTimeout;

    private static final Log logger = LogFactory.getLog(MinioS3Configuration.class);

    @Bean
    @ConditionalOnMissingBean
    public MinioClient minioClient () {
        MinioClient minioClient=null;
        try {
            logger.info("Connecting to " + endPoint);
            minioClient = new MinioClient(endPoint, accessKey, secretKey);
            minioClient.setTimeout(connectTimeout.longValue(), writeTimeout.longValue(), readTimeout.longValue());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return minioClient;
    }
}

