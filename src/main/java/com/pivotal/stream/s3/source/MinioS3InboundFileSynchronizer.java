package com.pivotal.stream.s3.source;

import io.minio.MinioClient;
import io.minio.ObjectStat;
import io.minio.messages.Item;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.file.filters.FileListFilter;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.remote.synchronizer.AbstractInboundFileSynchronizer;
import org.springframework.integration.metadata.SimpleMetadataStore;

/**
 * Created by mwright on 12/4/16.
 */
public class MinioS3InboundFileSynchronizer extends AbstractInboundFileSynchronizer<Item> {
    public MinioS3InboundFileSynchronizer() throws Exception {
        this(new MinioS3SessionFactory());
    }

    public MinioS3InboundFileSynchronizer(MinioClient minioClient) {
        this(new MinioS3SessionFactory(minioClient));
    }

    public MinioS3InboundFileSynchronizer(SessionFactory<Item> sessionFactory) {
        super(sessionFactory);
        setRemoteDirectoryExpression(new LiteralExpression(null));
    }
    @Override
    public final void setRemoteDirectoryExpression(Expression remoteDirectoryExpression) {
        super.setRemoteDirectoryExpression(remoteDirectoryExpression);
    }

    @Override
    public final void setFilter(FileListFilter<Item> filter) {
        super.setFilter(filter);
    }

    @Override
    protected boolean isFile(Item file) {
        return true;
    }

    @Override
    protected String getFilename(Item file) {
        return (file != null ? file.objectName() : null);
    }

    @Override
    protected long getModified(Item file) {
        return file.lastModified().getTime();
    }

}
