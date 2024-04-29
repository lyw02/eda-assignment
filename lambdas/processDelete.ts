import { SNSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, DeleteCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: SNSHandler = async (event) => {
  for (const record of event.Records) {
    const snsMessage = JSON.parse(record.Sns.Message);

    if (snsMessage.Records) {
      for (const messageRecord of snsMessage.Records) {
        const s3e = messageRecord.s3;
        const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));

        await ddbDocClient.send(
          new DeleteCommand({
            TableName: process.env.DYNAMODB_TABLE_NAME,
            Key: {
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
