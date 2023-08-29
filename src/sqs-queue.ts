import {SqsEventListener} from './sqs-event-listener';
import {ReceiveMessageCommandInput} from "@aws-sdk/client-sqs/dist-types/commands/ReceiveMessageCommand";
import {Message} from "@aws-sdk/client-sqs";
import { SQS } from "@aws-sdk/client-sqs";
import {Log} from "./log";

export interface ConsumerProps {
  eventListener: SqsEventListener;
  sqsClient: SQS;
  queueUrl: string;
  log: Log;
  isAsync?: boolean;
  visibilityTimeout?: number;
  messageAttributeNames?: string[];
  attributeNames?: string[];
  timeout?: number;

}

export class SqsQueue {

  protected readonly queueUrl: string;
  protected readonly isAsync: boolean;
  protected readonly visibilityTimeout: number;
  protected readonly messageAttributeNames: string[];
  protected readonly maxNumberOfMessages: number;
  protected readonly attributeNames: string[];
  protected readonly timeout: number;
  protected eventListener: SqsEventListener;
  protected sqsClient: SQS
  protected log: Log


  constructor(props: ConsumerProps) {
    this.isAsync = props.isAsync ?? true;
    this.maxNumberOfMessages = this.isAsync
      ? 10
      : 1;
    this.queueUrl = props.queueUrl;
    this.visibilityTimeout = props.visibilityTimeout ?? 15;
    this.messageAttributeNames = props.messageAttributeNames ?? ['All'];
    this.attributeNames = props.attributeNames ?? ['All'];
    this.timeout = props.timeout ?? 2
    this.sqsClient = props.sqsClient

    this.eventListener = props.eventListener
    this.eventListener.setQueueUrl(props.queueUrl)
    this.eventListener.setSqsClient(props.sqsClient)

    this.log = props.log
  }

  async start() {
    console.log('queue started')

    const params: ReceiveMessageCommandInput = {
      QueueUrl: this.queueUrl,
      VisibilityTimeout: this.visibilityTimeout,
      MessageAttributeNames: this.messageAttributeNames,
      MaxNumberOfMessages: this.maxNumberOfMessages,
      AttributeNames: this.attributeNames,
    };

    while (true) {
      try {
        await this.execute(params)
      } catch (e) {
        console.log(e)
        await new Promise(r => setTimeout(r, this.timeout * 1000));
      }
    }
  }

  async execute(params: ReceiveMessageCommandInput) {
    const messageResponse = await this.sqsClient.receiveMessage(params);

    if(!messageResponse.Messages) {
      await new Promise(r => setTimeout(r, this.timeout * 1000));
      return;
    }

    this.isAsync
      ? await this.consumeAsync(messageResponse.Messages)
      : await this.consume(messageResponse.Messages)

  }

  async consume(messageList: Message[]) {
    for (const message of messageList) {

      const body = await this.validateMessageBody(message.Body)

      if(body === false) {
        return await this.deleteEvent(message.ReceiptHandle)
      }

      const eventData = {receipt: message.ReceiptHandle, data: body.data};

      await this.eventListener.runEvent({ data: eventData, eventName: body.eventName, origin: body.origin});
    }
  }


  async consumeAsync(messageList: Message[]) {

    this.eventListener.resetEventList()

    for (const message of messageList) {

      const body = await this.validateMessageBody(message.Body)

      if(body === false) {
        return await this.deleteEvent(message.ReceiptHandle)
      }

      const eventData = {receipt: message.ReceiptHandle, data: body.data};

      this.eventListener.addEvent({ data: eventData, eventName: body.eventName, origin: body.origin});

    }

    await this.eventListener.runAllEvents();
  }

  async validateMessageBody(messageBody: string): Promise<BodyMessage|false> {
    try {
      const bodyRaw = JSON.parse(messageBody)

      const body = typeof bodyRaw.Message === 'string' ? JSON.parse(bodyRaw.Message) : bodyRaw

      if(!body.data || !body.eventName ||!body.origin) {
        await this.log.error('Message must be a json with properties {data: any, eventName: string, origin: string} and the current value is: ' + JSON.stringify(body))
        return false;
      }

      return {
        data: body.data,
        eventName: body.eventName,
        origin: body.origin
      }
    } catch (e) {
      await this.log.error('Message must be a json - current value is: ' + messageBody)
      return false;
    }
  }

  async deleteEvent(receipt: string) {
    await this.sqsClient.deleteMessage({
      QueueUrl: this.queueUrl,
      ReceiptHandle: receipt
    })
  }

}

interface BodyMessage {
  data: any,
  eventName: string
  origin: string

}