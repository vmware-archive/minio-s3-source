package com.pivotal.stream.s3.source;

import io.minio.MinioClient;
import io.minio.ObjectStat;
import io.minio.Result;
import io.minio.errors.*;
import io.minio.messages.Item;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.file.remote.session.Session;
import org.springframework.util.Assert;
import org.springframework.util.StreamUtils;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by mwright on 12/4/16.
 */
public class MinioS3Session implements Session<Item> {

    private final MinioClient minioClient;

    private static final Log logger = LogFactory.getLog(MinioS3Session.class);

//    public MinioS3Session(MinioClient minioClient) {
//        this(minioClient, null);
//    }

    public MinioS3Session(MinioClient minioClient) {
//        this.resourceIdResolver = resourceIdResolver;
        Assert.notNull(minioClient, "'minioClient' must not be null.");
        this.minioClient = minioClient;
    }

    @Override
    public boolean remove(String path) throws IOException {
        String[] bucketKey = splitPathToBucketAndPrefix(path);
        try {
            this.minioClient.removeObject(bucketKey[0], bucketKey[1]);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Failed to remove object", e.getCause());
        }
        return true;
    }

    @Override
    public Item[] list(String path) throws IOException {
        String[] bucketKey = splitPathToBucketAndPrefix(path);
        Iterable<Result<Item>> results;
        ArrayList<Item> items = new ArrayList<Item>();
        try {
            if (bucketKey.length > 1) {
                results = this.minioClient.listObjects(bucketKey[0], bucketKey[1]);
            } else {
                results = this.minioClient.listObjects(bucketKey[0]);
            }
            for (Iterator<Result<Item>> itr = results.iterator(); itr.hasNext();) {
                Result<Item> item = itr.next();
                items.add(item.get());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new IOException("Failed to list objects at " + path, ex.getCause());
        }

        return items.toArray(new Item[items.size()]);
    }

    @Override
    public void read(String path, OutputStream outputStream) throws IOException {
        String[] bucketKey = splitPathToBucketAndPrefix(path);
        try {
            InputStream is = this.minioClient.getObject(bucketKey[0], bucketKey[1]);
            //TODO: detect if a gzip extension - GZIPInputStream gzis = new GZIPInputStream(is);
            try {
                if (bucketKey[1].endsWith(".gz")) {
                    GZIPInputStream gzis = new GZIPInputStream(is);
                    StreamUtils.copy(gzis, outputStream);
                } else {
                    StreamUtils.copy(is, outputStream);
                }
            } finally {
                is.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Failed to read object " + path, e.getCause());
        }
    }

    @Override
    public void write(InputStream inputStream, String path) throws IOException {
        Assert.notNull(inputStream, "'inputStream' must not be null.");
        String[] bucketKey = splitPathToBucketAndPrefix(path);
        try {
            //TODO: not sure how specify size of file at this point
            this.minioClient.putObject(bucketKey[0], bucketKey[1], inputStream, 60 * 1024, null);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Failed to write object " + path, e.getCause());
        }
    }

    @Override
    public void append(InputStream inputStream, String s) throws IOException {
        throw new UnsupportedOperationException("The 'append' operation isn't supported by the Amazon S3 protocol.");
    }

    @Override
    public boolean mkdir(String directory) throws IOException {
        try {
            this.minioClient.makeBucket(directory);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Failed to create bucket " + directory, e.getCause());
        }
        return true;
    }

    @Override
    public boolean rmdir(String directory) throws IOException {
        try {
            this.minioClient.removeBucket(directory);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Failed to remove bucket " + directory, e.getCause());
        }
        return true;
    }

    @Override
    public void rename(String s, String s1) throws IOException {
        throw new UnsupportedOperationException("The 'rename' operation is not yet implemented");
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public boolean exists(String path) throws IOException {
        String[] bucketKey = splitPathToBucketAndPrefix(path);
        try {
            ObjectStat stat = this.minioClient.statObject(bucketKey[0],bucketKey[1]);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to check object " + path, e.getCause());
            return false;
        }
        return true;
    }

    @Override
    public String[] listNames(String path) throws IOException {
        String[] bucketKey = splitPathToBucketAndPrefix(path);
        Iterable<Result<Item>> results;
        ArrayList<String> itemNames = new ArrayList<String>();
        try {
            if (bucketKey.length > 1) {
                results = this.minioClient.listObjects(bucketKey[0], bucketKey[1]);
            } else {
                results = this.minioClient.listObjects(bucketKey[0]);
            }
            for (Iterator<Result<Item>> itr = results.iterator(); itr.hasNext();) {
                Result<Item> item = itr.next();
                itemNames.add(item.get().objectName());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new IOException("Failed to list object names", ex.getCause());
        }
        return itemNames.toArray(new String[itemNames.size()]);
    }

    @Override
    public InputStream readRaw(String path) throws IOException {
        String[] bucketKey = splitPathToBucketAndPrefix(path);
        InputStream is;
        try {
            is = this.minioClient.getObject(bucketKey[0], bucketKey[1]);
            //TODO: detect if a gzip extension - GZIPInputStream gzis = new GZIPInputStream(is);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Failed to read object " + path, e.getCause());
        }
        return is;
    }

    @Override
    public boolean finalizeRaw() throws IOException {
        return true;
    }

    @Override
    public Object getClientInstance() {
        return this.minioClient;
    }

    private String[] splitPathToBucketAndPrefix(String path) {
        Assert.hasText(path, "'path' must not be empty String.");
        String[] bucketKey = path.split("/", 2);



            Assert.state(bucketKey.length > 0 && bucketKey[0].length() >= 3,
                    "S3 bucket name must be at least 3 characters long.");

        return bucketKey;
    }

}
