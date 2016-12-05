# minio-s3-source
An alternative to the AWS-S3 source for Spring Cloud Dataflow, primarily to support on-prem S3 capable storage.  Uses Minio Java Client<br>
<br>
This stream source app was closely modeled after the AWS-S3 starter app.  Therefore much of the docs apply - http://docs.spring.io/spring-cloud-stream-app-starters/docs/1.1.0.BUILD-SNAPSHOT/reference/html/sources.html#spring-cloud-stream-modules-aws-s3-source <br>
<br>
The primary different is that this source supports specifying an API endpoint for issueing the requests.  Also added timeout settings for large file operations.<br>
<br>
To use:<br>
1. package the jar file and store in a Maven or S3 location.  Don't forget to make the jar public for access from Spring Cloud Dataflow.<br>
2. register the source from within SCDF shell<br>
  Ex: app register --name minio-s3 --type source --uri https://s3.amazonaws.com/scdf-apps-download/minio-s3-0.0.1-SNAPSHOT.jar<br>
3. create a stream from within the SCDF shell<br>
  Ex: stream create --name test-s3 --definition "minio-s3 --s3.endpoint='http://192.168.0.106:9000' --s3.accessKey='33K7297MUAP4F9RVXP0Z' --s3.secretKey='fI4pqXYQl8CkEwxOf8sjwGFvfKVOoBNwuOXMIPHF' --s3.remote-dir=sample-data --s3.connectTimeout=100000 --s3.readTimeout=100000 --s3.writeTimeout=100000 --file.consumer.mode=lines --s3.filename-pattern='*.csv' | log" --deploy<br>
  
