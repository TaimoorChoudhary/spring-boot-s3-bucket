package com.taimoor.spring.s3bucket;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author Taimoor Choudhary
 */
@Component
public class S3BucketManager {

    @Autowired
    private AmazonS3 amazonS3Client;

    @Value("${aws.services.bucket}")
    private String bucketName;

    /**
     * Uploaded file to S3 bucket using amazon S3 client
     * @param fileName
     * @param file
     * @param bucketName
     */
    public void uploadFile(final String fileName, final MultipartFile file,
                                      final String bucketName, final boolean allowPublicAccess) {

        final ObjectMetadata data = new ObjectMetadata();
        data.setContentType(file.getContentType());
        data.setContentLength(file.getSize());

        try {
            final PutObjectRequest putObjectRequest;
            if(allowPublicAccess){
                putObjectRequest = new PutObjectRequest(bucketName, fileName, file.getInputStream(), data)
                        .withCannedAcl(CannedAccessControlList.PublicRead);
            }else {
                putObjectRequest = new PutObjectRequest(bucketName, fileName, file.getInputStream(), data);
            }

            amazonS3Client.putObject(putObjectRequest);
        } catch (IOException e) {

        }
    }

    /**
     * Downloads file from S3 bucket using amazon S3 client
     * @param fileName
     * @param bucketName
     * @return
     */
    public S3Object downloadFile(final String fileName, final String bucketName) {
        return amazonS3Client.getObject(bucketName,  fileName);
    }

    /**
     * Deletes file from S3 bucket using amazon S3 client
     * @param fileName
     * @param bucketName
     */
    public void deleteFile(final String fileName, final String bucketName) {
        amazonS3Client.deleteObject(bucketName, fileName);
    }

    /**
     * Downloads Image file as File Object
     * @param fileName
     * @return
     */
    public File downloadImage(final String fileName) {

        try {
            final S3Object s3Object = downloadFile(fileName, bucketName);
            final S3ObjectInputStream inputStream = s3Object.getObjectContent();

            byte[] bytes = null;
            File file = new File(fileName);
            try {
                bytes = StreamUtils.copyToByteArray(inputStream);
                FileOutputStream fileOutputStream = new FileOutputStream(file);
                fileOutputStream.write(bytes);
            } catch (IOException e) {
                /* Handle Exception */
            }
            return file;
        }catch (AmazonS3Exception amazonS3Exception){
            throw amazonS3Exception;
        }
    }

    public ResponseEntity downloadImageAsResponseEntity(final String fileName) {

        try {
            final S3Object s3Object = downloadFile(fileName, bucketName);
            final S3ObjectInputStream inputStream = s3Object.getObjectContent();

            byte[] bytes = null;
            try {
                bytes = StreamUtils.copyToByteArray(inputStream);
            } catch (IOException e) {
                /* Handle Exception */
            }

            String contentType = s3Object.getObjectMetadata().getContentType();
            return ResponseEntity.ok().contentType(MediaType.parseMediaType(contentType)).body(bytes);
        }catch (AmazonS3Exception amazonS3Exception){
            throw amazonS3Exception;
        }
    }
}
