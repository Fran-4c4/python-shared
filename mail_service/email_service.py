'''
- author: Francisco R. Moreno Santana franrms@gmail.com
'''
import json
import logging

from common.configurations import AppConfigEnum
from dbhelper.DBBase import DBBase

from .MailTplConfig import MailTplConfig

import datetime as dt
# from gshared.configurations import AppConfigEnum

class EmailService(DBBase):

    def __init__(self,scoped_session=None, config={}):
        super().__init__(scoped_session)
        self.log= logging.getLogger(__name__)
        self.config=config
        self.QueueFactory=None
        if config is not None:
            self.queue_email_config = self.config.get("QUEUE_EMAIL_CONFIG")
        
    def get_template_configuration(self, template_id):
        session=self.getsession()
        data=session.query(MailTplConfig). \
                filter(MailTplConfig.id == template_id).one()  # data must exist 
        return data  
    
    def send_email_by_template(self, recipient_list:list,email_subject:str,template_id:str,ruser, template_data):     
        tpl=self.get_template_configuration(template_id=template_id)        
        message=self.template_replace_data(template=tpl.tpl_data,template_data=template_data)
        self.send_email(recipient_list=recipient_list,message=message,email_subject=email_subject,ruser=ruser)
        
    def template_replace_data(self,template, template_data):
        """
        Replaces placeholders in the template with values from the template_data dictionary.

        :param template: str, the template string with placeholders (e.g., "{{data_1}}")
        :param template_data: dict, a dictionary containing placeholder keys and their replacement values
        :return: str, the updated template with placeholders replaced
        """
        if template_data is None:
            return template
        
        for key, value in template_data.items():
            placeholder = f"{{{{{key}}}}}"  # Create the placeholder format "{{key}}"
            template = template.replace(placeholder, value)
            
        return template

    def send_email(self, recipient_list:list,message:str,email_subject:str ,ruser):
        """_summary_

        Args:
            recipient_list (list): _description_
            message (str): _description_
            email_subject (str): _description_
            ruser (_type_): _description_
        """        


        #send email to user... directly or using a queue
        if self.QueueFactory is None:
            raise Exception ("QueueFactory was not set")
        qmsg={}
        qmsg["sendto"]=recipient_list #email
        qmsg["subject"]=email_subject
        qmsg["body_html"]=message
        jmsg=json.dumps(qmsg)
        #send to queue
        _queue=self.QueueFactory(self.queue_email_config).tool
        _queue.send_message(jmsg)


