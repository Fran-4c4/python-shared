

import logging
from gshared.exceptions import  DBException
from .DBBase import  DBBase
from gshared.modelhelper import Error

class ErrorDB(DBBase):
    """class to manage errors for application


    Args:
        DBBase (DBBase): DBBase
    """    

    def __init__(self,scoped_session=None):
        super().__init__(scoped_session)
        self.log= logging.getLogger(__name__)

    def list(self)->list:
        """
        Returns the list with the message error in the DB.

        Raises:
            DBException: SQLAlchemyError
            DBException: Exception

        Returns:
            list: Error list.
        """      
        session = self.getsession()
        try:
            item = session.query(Error).all()
            return item

        except Exception as oEx:
            self.log.error(str(oEx))
            raise DBException('1.1.4.1', 'list',
                              'Generic Error retrieving list of errors: %s' % str(oEx))
        finally:
            self.closeSession()