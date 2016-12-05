package com.pivotal.stream.s3.source;

import io.minio.messages.Item;
import org.springframework.integration.file.filters.AbstractSimplePatternFileListFilter;

/**
 * Created by mwright on 12/4/16.
 */
public class MinioS3SimplePatternFileListFilter extends AbstractSimplePatternFileListFilter<Item> {
    public MinioS3SimplePatternFileListFilter(String path) {
        super(path);
    }

    @Override
    protected String getFilename(Item file) {
        return (file != null) ? file.objectName() : null;
    }
}
