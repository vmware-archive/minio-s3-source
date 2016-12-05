package com.pivotal.stream.s3.source;

import io.minio.ObjectStat;
import io.minio.messages.Item;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.remote.synchronizer.AbstractInboundFileSynchronizer;
import org.springframework.integration.file.remote.synchronizer.AbstractInboundFileSynchronizingMessageSource;
import org.springframework.messaging.Message;

import java.io.File;
import java.io.InputStream;
import java.util.Comparator;

/**
 * Created by mwright on 12/1/16.
 */
public class MinioS3MessageSource extends AbstractInboundFileSynchronizingMessageSource<Item> {

    public MinioS3MessageSource(AbstractInboundFileSynchronizer<Item> synchronizer) {
        super(synchronizer);
    }

    public MinioS3MessageSource(AbstractInboundFileSynchronizer<Item> synchronizer, Comparator<File> comparator) {
        super(synchronizer, comparator);
    }

    @Override
    public String getComponentType() {
        return "minio:s3-inbound-channel-adapter";
    }
}
