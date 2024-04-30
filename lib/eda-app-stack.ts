import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";

import { Construct } from "constructs";
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class EDAAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 bucket
    const imagesBucket = new s3.Bucket(this, "images", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      publicReadAccess: false,
    });

    // Dynamo DB table
    const imagesTable = new dynamodb.Table(this, "ImagesTable", {
      partitionKey: { name: "filename", type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      tableName: "Images",
    });

    // Integration infrastructure
    const dlq = new sqs.Queue(this, "dead-letter-queue", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
    });

    const imageProcessQueue = new sqs.Queue(this, "img-created-queue", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
      deadLetterQueue: {
        queue: dlq,
        maxReceiveCount: 3,
      },
    });

    // Lambda functions
    const processImageFn = new lambdanode.NodejsFunction(
      this,
      "ProcessImageFn",
      {
        runtime: lambda.Runtime.NODEJS_18_X,
        entry: `${__dirname}/../lambdas/processImage.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
        environment: {
          DLQ_URL: dlq.queueUrl,
        },
      }
    );

    const confirmationMailerFn = new lambdanode.NodejsFunction(
      this,
      "confirmationMailer-function",
      {
        runtime: lambda.Runtime.NODEJS_16_X,
        memorySize: 1024,
        timeout: cdk.Duration.seconds(3),
        entry: `${__dirname}/../lambdas/confirmationMailer.ts`,
      }
    );

    const rejectionMailerFn = new lambdanode.NodejsFunction(
      this,
      "RejectionMailerFunction",
      {
        runtime: lambda.Runtime.NODEJS_16_X,
        entry: `${__dirname}/../lambdas/rejectionMailer.ts`,
        timeout: cdk.Duration.seconds(3),
        memorySize: 1024,
      }
    );

    const processDeleteFn = new lambdanode.NodejsFunction(
      this,
      "ProcessDeleteFn",
      {
        runtime: lambda.Runtime.NODEJS_16_X,
        entry: `${__dirname}/../lambdas/processDelete.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 256,
        environment: {
          DYNAMODB_TABLE_NAME: imagesTable.tableName,
        },
      }
    );

    const updateTableFn = new lambdanode.NodejsFunction(this, "updateTableFn", {
      runtime: lambda.Runtime.NODEJS_18_X,
      entry: `${__dirname}/../lambdas/updateTable.ts`,
      timeout: cdk.Duration.seconds(15),
      memorySize: 128,
      environment: {
        DYNAMODB_TABLE_NAME: imagesTable.tableName,
      },
    });

    // Topics
    const newImageTopic = new sns.Topic(this, "NewImageTopic", {
      displayName: "New Image topic",
    });

    const deleteAndUpdateTopic = new sns.Topic(this, "DeleteAndUpdateTopic", {
      displayName: "Delete and Update Topic",
    });

    newImageTopic.addSubscription(
      new subs.LambdaSubscription(confirmationMailerFn)
    );
    newImageTopic.addSubscription(new subs.SqsSubscription(imageProcessQueue));

    deleteAndUpdateTopic.addSubscription(
      new subs.LambdaSubscription(processDeleteFn, {
        filterPolicy: {
          comment_type: sns.SubscriptionFilter.stringFilter({
            allowlist: ["Delete"],
          }),
        },
      })
    );
    deleteAndUpdateTopic.addSubscription(
      new subs.LambdaSubscription(updateTableFn, {
        filterPolicy: {
          comment_type: sns.SubscriptionFilter.stringFilter({
            allowlist: ["Caption"],
          }),
        },
      })
    );

    // S3 --> SQS
    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(newImageTopic)
    );

    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_REMOVED_DELETE,
      new s3n.SnsDestination(deleteAndUpdateTopic)
    );

    // SQS --> Lambda
    processImageFn.addEventSource(
      new events.SqsEventSource(imageProcessQueue, {
        batchSize: 5,
        maxBatchingWindow: cdk.Duration.seconds(10),
      })
    );

    // DLQ --> Lambda
    rejectionMailerFn.addEventSource(
      new events.SqsEventSource(dlq, {
        batchSize: 5,
        maxBatchingWindow: cdk.Duration.seconds(10),
      })
    );

    // Dynamo DB Permissions
    processImageFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["dynamodb:PutItem", "dynamodb:GetItem"],
        resources: [imagesTable.tableArn],
      })
    );

    processImageFn.addEnvironment("DYNAMODB_TABLE_NAME", imagesTable.tableName);
    imagesTable.grantReadWriteData(processImageFn);
    imagesTable.grantWriteData(processDeleteFn);
    imagesTable.grantReadWriteData(updateTableFn);

    // Permissions
    imagesBucket.grantRead(processImageFn);

    processImageFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["sqs:SendMessage"],
        resources: [dlq.queueArn],
      })
    );

    confirmationMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    rejectionMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    processDeleteFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["dynamodb:DeleteItem"],
        resources: [imagesTable.tableArn],
      })
    );

    updateTableFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["dynamodb:UpdateItem"],
        resources: [imagesTable.tableArn],
      })
    );

    // Output
    new cdk.CfnOutput(this, "bucketName", {
      value: imagesBucket.bucketName,
    });
    new cdk.CfnOutput(this, "newImageTopicARN", {
      value: newImageTopic.topicArn,
    });
    new cdk.CfnOutput(this, "deleteAndUpdateTopicARN", {
      value: deleteAndUpdateTopic.topicArn,
    });
  }
}
