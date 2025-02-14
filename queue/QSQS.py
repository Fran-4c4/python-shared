
from typing import Dict
import boto3
import logging
import uuid
import json
import datetime as dt


class QSQS:

    def __init__(self, config:Dict):  
        """init class

        Args:
            config (dict): dict with this data: queueName, messageGroupId, aws_region_name

        Raises:
            Exception: _description_
        """            
        self.log = logging.getLogger(__name__)
        self.queueName=config.get('queueName')
        self.messageGroupId= config.get('messageGroupId')
        self.aws_region_name=config.get('aws_region_name')

        # Get the service resource
        try:
            self.sqs = boto3.resource('sqs',region_name=self.aws_region_name) #Basura de amazon, no dice en ningun lugar que haya que poner la region. Las pruebas en local funcionan porque lo coge de la IAM
            self.queueName = self.queueName
        except Exception as oEx:
            raise Exception("Queue initialization failed: " + str(oEx))

        
    def config(self, queueName=None,aws_region_name='eu-west-1',messageGroupId="GLOBAL"):
        """helper config

        Args:
            queueName (string): queue Name. Defaults to None.
            aws_region_name (str, optional): AWS region. Defaults to 'eu-west-1'.
            messageGroupId (string) : The name of group . Defaults to 'GLOBAL'
        Raises:
            Exception: Exception
        """          
        self.queueName=queueName
        self.messageGroupId= messageGroupId
        self.aws_region_name=aws_region_name 

           

    def send_message(self,  messageBody:str=None, queueName:str=None, messageGroupId:str=None,messageDeduplicationId:str=None):
        """send message to queue
        Optional values take main class values
        MessageBody is the body that receivers will use

        Args:
            messageBody (str, optional): message Body. Defaults to None.
            queueName (str, optional): queue Name. Defaults to None.            
            messageGroupId (str, optional): messageGroupId. Defaults to None.
            messageDeduplicationId (str, optional): messageDeduplicationId. Defaults to None.

        Raises:
            oEx: error

        Returns:
            aws-sqs response: sqs response
        """        
        _sqs = self.sqs
        queue = None
        try:            
            if queueName is None:
                queueName = self.queueName
            if messageGroupId is None:
                messageGroupId = self.messageGroupId                
            if messageDeduplicationId is None:
                messageDeduplicationId=str(uuid.uuid4())
            # Get the queue. This returns an SQS.Queue instance
            queue = _sqs.get_queue_by_name(QueueName=queueName)
            # Create a new message
            response = queue.send_message(MessageBody=messageBody,MessageGroupId=messageGroupId,MessageDeduplicationId=messageDeduplicationId)
            # The response is NOT a resource, but gives you a message ID and MD5
        except Exception as oEx:
            raise oEx #Exception("Queue: " + queue + " " + str(oEx))

        return response
