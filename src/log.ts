import { SNS } from "@aws-sdk/client-sns";

interface LogData {
  msg: string,
  originName: string,
  level: 'ERROR' | 'DEBUG' | 'WARNING' | 'INFO'
}
interface LogProps {
  applicationId?: string

  sns?: {
    client: SNS,
    topicArn: string,
  }

}
export class Log {
  constructor(protected props: LogProps = {}) {}

  async handleLog(data: LogData) {

    console.log(data)

    if(!this.props.sns) {
      return
    }

    await this.props.sns.client.publish({
      TopicArn: this.props.sns.topicArn,
      Message: JSON.stringify({
        eventName: 'logCreated',
        origin: data.originName,
        data: { message: data.msg, applicationId: this.props.applicationId, level: data.level }
      })
    })
  }

  async error(msg: string, originName = '') {
    await this.handleLog({
      msg,
      level: 'ERROR',
      originName
    })
  }

  async warning(msg: string, originName = '') {
    await this.handleLog({
      msg,
      level: 'WARNING',
      originName
    })
  }

  async info(msg: string, originName = '') {
    await this.handleLog({
      msg,
      level: 'INFO',
      originName
    })
  }

  async debug(msg: string, originName = '') {
    await this.handleLog({
      msg,
      level: 'DEBUG',
      originName
    })
  }
}