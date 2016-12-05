package com.pivotal.stream.s3.source;

/**
 * Created by mwright on 12/1/16.
 */
import io.minio.MinioClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.file.FileConsumerProperties;
import org.springframework.cloud.stream.app.file.FileUtils;
import org.springframework.cloud.stream.app.trigger.TriggerConfiguration;
import org.springframework.cloud.stream.app.trigger.TriggerPropertiesMaxMessagesDefaultUnlimited;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.util.StringUtils;

@EnableBinding(Source.class)
@EnableConfigurationProperties({ MinioS3SourceProperties.class, FileConsumerProperties.class,
        TriggerPropertiesMaxMessagesDefaultUnlimited.class})
@Import({ TriggerConfiguration.class, MinioS3Configuration.class })
public class MinioS3SourceConfiguration {
    @Autowired
    private MinioS3SourceProperties s3SourceProperties;

    @Bean
    public MinioS3InboundFileSynchronizer s3InboundFileSynchronizer(MinioClient minioClient) {
        MinioS3SessionFactory s3SessionFactory = new MinioS3SessionFactory(minioClient);
        MinioS3InboundFileSynchronizer synchronizer = new MinioS3InboundFileSynchronizer(s3SessionFactory);
        synchronizer.setDeleteRemoteFiles(this.s3SourceProperties.isDeleteRemoteFiles());
        synchronizer.setPreserveTimestamp(this.s3SourceProperties.isPreserveTimestamp());
        String remoteDir = this.s3SourceProperties.getRemoteDir();
        synchronizer.setRemoteDirectory(remoteDir);
        synchronizer.setRemoteFileSeparator(this.s3SourceProperties.getRemoteFileSeparator());
        synchronizer.setTemporaryFileSuffix(this.s3SourceProperties.getTmpFileSuffix());

        if (StringUtils.hasText(this.s3SourceProperties.getFilenamePattern())) {
            synchronizer.setFilter(new MinioS3SimplePatternFileListFilter(this.s3SourceProperties.getFilenamePattern()));
        }
        else if (this.s3SourceProperties.getFilenameRegex() != null) {
            synchronizer.setFilter(new MinioS3RegexPatternFileListFilter(this.s3SourceProperties.getFilenameRegex()));
        }

        return synchronizer;

    }

    @Bean
    public IntegrationFlow s3InboundFlow(FileConsumerProperties fileConsumerProperties, MinioS3InboundFileSynchronizer s3InboundFileSynchronizer) {
        MinioS3MessageSource s3MessageSource =
                new MinioS3MessageSource(s3InboundFileSynchronizer);
        s3MessageSource.setLocalDirectory(this.s3SourceProperties.getLocalDir());
        s3MessageSource.setAutoCreateLocalDirectory(this.s3SourceProperties.isAutoCreateLocalDir());

        return FileUtils.enhanceFlowForReadingMode(IntegrationFlows.from(s3MessageSource), fileConsumerProperties)
                .channel(Source.OUTPUT)
                .get();
    }
}
