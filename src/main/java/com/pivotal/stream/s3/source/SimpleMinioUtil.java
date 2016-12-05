package com.pivotal.stream.s3.source;

import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by mwright on 12/5/16.
 */
public class SimpleMinioUtil {
    private static MinioClient minioClient;

    private static String endPoint = "http://192.168.0.106:9000";

    private static String accessKey = "33K7297MUAP4F9RVXP0Z";

    private static String secretKey = "fI4pqXYQl8CkEwxOf8sjwGFvfKVOoBNwuOXMIPHF";

    private static Integer connectTimeout = 100000;

    private static Integer readTimeout = 100000;

    private static Integer writeTimeout = 100000;

    public static void main(String[] args) throws Exception {
        Iterable<Result<Item>> results;
        ArrayList<String> itemNames = new ArrayList<String>();
        try {

                results = SimpleMinioUtil.minioClient().listObjects("sample-data", "HEDIS-2016/Events");

            for (Iterator<Result<Item>> itr = results.iterator(); itr.hasNext();) {
                Result<Item> item = itr.next();
                itemNames.add(item.get().objectName());
                System.out.println(item.get().objectName());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new IOException("Failed to list object names", ex.getCause());
        }
    }

    public static MinioClient minioClient () {
        if (minioClient!=null)
            return minioClient;

        try {
//            logger.info("Connecting to " + endPoint);
            minioClient = new MinioClient(endPoint, accessKey, secretKey);
            minioClient.setTimeout(connectTimeout.longValue(), writeTimeout.longValue(), readTimeout.longValue());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return minioClient;
    }
}
