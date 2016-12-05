package com.pivotal.stream.s3.source;

import org.hibernate.validator.constraints.Length;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.app.file.remote.AbstractRemoteFileSourceProperties;

import java.io.File;

/**
 * Created by mwright on 12/1/16.
 */
@ConfigurationProperties("s3")
public class MinioS3SourceProperties extends AbstractRemoteFileSourceProperties {
    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    private String endpoint;

    public MinioS3SourceProperties() {
        setRemoteDir("bucket");
        setLocalDir(new File(System.getProperty("java.io.tmpdir") + "/s3/source"));
        setEndpoint("http://localhost:9000");
    }

    @Override
    @Length(min = 3)
    public String getRemoteDir() {
        return super.getRemoteDir();
    }
}