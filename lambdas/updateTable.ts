import { SNSHandler } from "aws-lambda";
import { DynamoDBDocumentClient, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: SNSHandler = async (event) => {
  for (const record of event.Records) {
    const message = JSON.parse(record.Sns.Message);
    const messageName = message.name;
    const messageDescription = message.description;

    await ddbDocClient.send(
      new UpdateCommand({
        TableName: process.env.DYNAMODB_TABLE_NAME,
        Key: { filename: messageName },
        UpdateExpression: "SET description = :d",
        ExpressionAttributeValues: {
          ":d": messageDescription,
        },
      })
    );
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
