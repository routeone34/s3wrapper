import { S3Client, GetObjectCommand, DeleteObjectCommand, ListObjectsCommand, PutObjectCommand, CopyObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";

export const s3wrapper = {
    getClient: function() {
        return new S3Client({
            region: process.env.S3_REGION,
            endpoint: process.env.S3_ENDPOINT,
            credentials: {
                accessKeyId: process.env.S3_ACCESS_KEY_ID,
                secretAccessKey: process.env.S3_SECRET_ACCESS_KEY
            },
        });
    },
    listObject: async function(prefix) {
        const client = this.getClient();
        const bucket = process.env.S3_BUCKET;
        const response = await client.send(
            new ListObjectsCommand({
                Bucket: bucket,
                Prefix: prefix
            })
        );
        return response;
    },
    getObject: async function(key, type) {
        const client = this.getClient();
        const bucket = process.env.S3_BUCKET;
        const response = await client.send(
            new GetObjectCommand({
                Bucket: bucket,
                Key: key
            })
        );
 
        switch(type) {
            case "json":
                const text = await new Response(response.Body).text();
                return JSON.parse(text);
                break;
            case "blob":
                const blob = await new Response(response.Body).blob();
                return blob;
            case "response":
                return response;
        }
    },
    deleteObject: async function(key) {
        const client = this.getClient();
        const bucket = process.env.S3_BUCKET;
        await client.send(
            new DeleteObjectCommand({
                Bucket: bucket,
                Key: key
            })
        );
        return "OK";
    },
    putObject: async function(key, buffer) {
        const client = this.getClient();
        const bucket = process.env.S3_BUCKET;
        await client.send(
            new PutObjectCommand({
                Body: buffer,
                Bucket: bucket,
                Key: key
            })
        );
    },
    copyObject: async function(key1, key2) {
        const client = this.getClient();
        const bucket = process.env.S3_BUCKET;
        await client.send(
            new CopyObjectCommand({
                CopySource: encodeURI(bucket + "/" + key1),
                Bucket: bucket,
                Key: key2
            })
        );
    },
    uploadByStream: async function(stream, key) {
        const client = this.getClient();
        const bucket = process.env.S3_BUCKET;
        const upload = new Upload({
        client: client,
            params: {
                Bucket: bucket, 
                Key: key,
                Body: stream,
                ContentType: "application/octet-stream",
            },
        });
        await upload.done();
    }
};
export default s3wrapper;
