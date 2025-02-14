

import logging

from sqlalchemy.exc import SQLAlchemyError
from gshared.exceptions import  DBException
from .DBBase import  DBBase
from gshared.modelhelper import Configuration

class ConfigurationDB(DBBase):

    def __init__(self,scoped_session=None):
        super().__init__(scoped_session)
        self.log= logging.getLogger(__name__)

    def get(self, key:str)-> Configuration:
        """
        Return a specific Configuration according to the key.

        Args:
            key (str): Configuration key

        Raises:
            DBException: SQLAlchemyError exception.
            DBException: Exception exception.

        Returns:
            Configuration: Configuration DB model object.
        """
        session = self.getsession()
        try:
            item = session.query(Configuration).filter(Configuration.key==str(key)).first()
            self.closeSession()           
            return item
        except SQLAlchemyError as oEx:
            self.log.error(str(oEx))
            raise DBException('1.2.0.0', 'get', 'Error retrieving Configuration: %s' % str(oEx.args))
        except Exception as oEx:
            self.log.error(str(oEx))
            raise DBException('1.2.0.1', 'get', 'Generic Error retrieving Configuration: %s' % str(oEx))
        finally:
            self.closeSession()