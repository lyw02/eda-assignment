/* eslint-disable import/extensions, import/no-absolute-path */
import { SQSHandler } from "aws-lambda";
import {
  GetObjectCommand,
  PutObjectCommandInput,
  GetObjectCommandInput,
  S3Client,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { PutCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

const s3 = new S3Client();
const sqs = new SQSClient();
const ddbDocClient = createDDbDocClient();

export const handler: SQSHandler = async (event) => {
  console.log("Event ", JSON.stringify(event));
  for (const record of event.Records) {
    const recordBody = JSON.parse(record.body); // Parse SQS message
    const snsMessage = JSON.parse(recordBody.Message); // Parse SNS message

    if (snsMessage.Records) {
      console.log("Record body ", JSON.stringify(snsMessage));
      for (const messageRecord of snsMessage.Records) {
        const s3e = messageRecord.s3;
        const srcBucket = s3e.bucket.name;
        // Object key may have spaces or unicode non-ASCII characters.
        const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));

        // Check the file extension
        const extension = srcKey.match(/\.([^.]*)$/)![1].toLowerCase();
        if (extension !== "jpeg" && extension !== "png") {
          throw new Error("Unsupported file extension");
        }

        // Add to DynamoDB
        await ddbDocClient.send(
          new PutCommand({
            TableName: process.env.DYNAMODB_TABLE_NAME,
            Item: {
              filename: srcKey,
            },
          })
        );
      }
    }
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });

  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };

  const unmarshallOptions = {
    wrapNumbers: false,
  };

  const translateConfig = { marshallOptions, unmarshallOptions };

  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}

//////////////////////////////
