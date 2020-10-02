using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace AwsSqsTestApp
{
    class Program
    {
        static void Main(string[] args)
        {
            AmazonSQSClient sqsClient = new AmazonSQSClient();
            string queueUrl = null;
            CreateQueueRequest sqsCreateReq = new CreateQueueRequest(); // request nesnemiz
            Dictionary<string, string> attr = new Dictionary<string, string>();


            sqsCreateReq.QueueName = "ConsoleAppTestQueue";
            attr.Add(QueueAttributeName.MaximumMessageSize, 256 * 1024 + ""); // byte
            attr.Add(QueueAttributeName.VisibilityTimeout, TimeSpan.FromHours(6).TotalSeconds.ToString()); //saniye
            sqsCreateReq.Attributes = attr;

            try
            {
                CreateQueueResponse res = sqsClient.CreateQueue(sqsCreateReq);

                if (res.HttpStatusCode == HttpStatusCode.OK)
                {
                    queueUrl = res.QueueUrl;
                    Console.WriteLine("Yeni Kuyruk Başarıyla Oluştu");
                    Console.WriteLine(queueUrl);
                }
                else
                {
                    Console.WriteLine("Kuyruk Oluşturulamadı .!");
                }
            }
            catch (Exception e)
            {

                Console.WriteLine("Kuyruk Oluşturma İşlemi Sırasında Bir Hata Oluştu .!");
                Console.WriteLine(e.Message);
            }

            if (!String.IsNullOrEmpty(queueUrl))
            {

                SendMessageRequest sqsMsgReq = new SendMessageRequest(); // mesaj request nesnemiz
                Dictionary<string, MessageAttributeValue> msgAtrr = new Dictionary<string, MessageAttributeValue>();
                SqsMessageBodyModel msgBody = new SqsMessageBodyModel();

                msgAtrr.Add("Name", new MessageAttributeValue { DataType = "String", StringValue = "Ahmet Mirza Yıldırım" });

                msgBody.Id = Guid.NewGuid();
                msgBody.Age = 27;
                msgBody.Name = "Ahmet Mirza";
                msgBody.Surname = "Yıldırım";

                sqsMsgReq.MessageAttributes = msgAtrr;
                sqsMsgReq.DelaySeconds = 0; //sn
                sqsMsgReq.MessageBody = JsonConvert.SerializeObject(msgBody);
                sqsMsgReq.QueueUrl = queueUrl;


                SendMessageResponse msgRes = sqsClient.SendMessage(sqsMsgReq);
                ReceiveMessageRequest recMsgReq = new ReceiveMessageRequest();

                recMsgReq.MaxNumberOfMessages = 1;
                recMsgReq.QueueUrl = queueUrl;

                //SqsMessageBodyModel revMsgBody = new SqsMessageBodyModel();

                // kuyruk sürekli olarak dinleniyor.
                while (true)
                {
                    ReceiveMessageResponse revMsgRes = sqsClient.ReceiveMessage(recMsgReq);

                    if (revMsgRes.Messages.Count > 0)
                    {
                        Message msg = revMsgRes.Messages.First();

                        Console.WriteLine("     Message ID : " + msg.MessageId + "\n");
                        //revMsgBody = JsonConvert.DeserializeObject<SqsMessageBodyModel>(msg.Body);
                        Console.WriteLine("     " + msg.Body + "\n");

                        DeleteMessageRequest delMsgReq = new DeleteMessageRequest
                        {
                            QueueUrl = queueUrl,
                            ReceiptHandle = msg.ReceiptHandle
                        };

                        DeleteMessageResponse delMsgRes = sqsClient.DeleteMessage(delMsgReq);
                        if (delMsgRes.HttpStatusCode == HttpStatusCode.OK)
                        {
                            Console.WriteLine("     ===> Bu Mesaj Silindi");
                        }
                        Console.WriteLine("------------------------------------------------------------------------");
                    }
                }
            }
        }
    }
}

