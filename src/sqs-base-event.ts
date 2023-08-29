import { SQS } from "@aws-sdk/client-sqs";

interface EventData {
  receipt: string
}

export abstract class SqsBaseEvent<T extends EventData> {
  abstract handle(data: T): Promise<void>

  async execute(eventData: T, queueUrl: string, sqsClient: SQS): Promise<void> {

    await this.handle(eventData)

    await sqsClient
      .deleteMessage({
        QueueUrl: queueUrl,
        ReceiptHandle: eventData.receipt,
      })
  }

}