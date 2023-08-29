import {SqsBaseEvent} from './sqs-base-event';
import {SQS} from "@aws-sdk/client-sqs";

type Listeners = {
  [key: string]: SqsBaseEvent<any>
}

type Event = {
  eventName: string,
  origin: string
  data: any
}

export class SqsEventListener {

  protected readonly LISTENERS: Listeners = {};

  public eventList: Event[] = [];

  public queueUrl: string;

  public sqsClient: SQS;


  constructor(listeners?: Listeners) {
    if(listeners) {
      this.LISTENERS = listeners
    }
  }

  public getListenerByEvent(name: string): SqsBaseEvent<any> {
    const listeners = this.LISTENERS[name]

    return listeners ?? null
  }

  async runEvent(event: Event) {
    const listener = this.getListenerByEvent(event.eventName)

    if(!listener) {
      console.log('event not found. eventName: ' + event.eventName + ' from origin: ' + event.origin)
      return
    }

    await listener.execute(event.data, this.queueUrl, this.sqsClient);
  }


  resetEventList() {
    this.eventList = []
  }

  addEvent(event: Event) {
    this.eventList.push(event)
  }

  async runAllEvents() {

    const promises = [];

    for (const e of this.eventList) {

      const listener = this.getListenerByEvent(e.eventName)

      if(!listener) {
        console.log('event not found. eventName: ' + e.eventName + ' from origin: ' + e.origin)
        continue
      }

      promises.push(listener.execute(e.data, this.queueUrl, this.sqsClient))
    }

    this.resetEventList()

    await Promise.all(promises);
  }

  setQueueUrl(queueUrl: string): void {
    this.queueUrl = queueUrl;
  }

  setSqsClient(sqsClient: SQS): void {
    this.sqsClient = sqsClient;
  }
}
