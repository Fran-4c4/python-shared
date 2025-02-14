

import logging
from datetime import datetime as dt
from typing import Any


class QueueFactory:


    def __init__(self,config, log=None):        
        if log is not None:
            self.log= log
        else:
            self.log= logging.getLogger(__name__)

        self.config=config
        self.tool_type=self.config.get('tool_type')
        self.create_tool()


    
    def create_tool(self):
        """
        Return the queue object corresponding to tool_type

        Raises:
            Exception: Queue typenot found.

        Returns:
            _type_: Queue Type)
        """
        if (self.tool_type=="SQS"):
            from .QSQS import QSQS
            self.tool=QSQS(self.config)
            return self.tool
        else:
            raise Exception("QUEUE tool not found") 
