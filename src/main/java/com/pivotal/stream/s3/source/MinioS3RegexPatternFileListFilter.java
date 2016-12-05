package com.pivotal.stream.s3.source;

import io.minio.messages.Item;
import org.springframework.integration.file.filters.AbstractRegexPatternFileListFilter;

import java.util.regex.Pattern;

/**
 * Created by mwright on 12/4/16.
 */
public class MinioS3RegexPatternFileListFilter extends AbstractRegexPatternFileListFilter<Item> {
    public MinioS3RegexPatternFileListFilter(String pattern) {
        super(pattern);
    }

    public MinioS3RegexPatternFileListFilter(Pattern pattern) {
        super(pattern);
    }

    @Override
    protected String getFilename(Item file) {
        return (file != null) ? file.objectName() : null;
    }
}
