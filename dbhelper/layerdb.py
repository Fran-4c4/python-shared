
import logging
from db.DBErrors import DBE
from gshared.exceptions.DBException import DBException
from .DBBase import DBBase


class LayerDB(DBBase):

    def __init__(self,scoped_session=None):
        super().__init__(scoped_session)
        self.log= logging.getLogger(__name__)


    def get_base_layers(self):
        # log = logging.getLogger(__name__)
        session = self.getsession()
        try:        
            sSQL = f"SELECT * FROM layer "
            ret=session.execute(sSQL).fetchall()         
            return ret
        except Exception as oEx:
            raise DBException(code=DBE.ERR_LAYERS_1200,exception= oEx,logger=self.log)
        finally:
            self.closeSession()
           