'''
- author: Angel A. Velazquez R. angel.velazquez@geoaitech.com
@copyright: (c) 2022 Angel A. Velazquez R. angel.velazquez@geoaitech.com
@contact: angel.velazquez@geoaitech.com

'''

from gshared.exceptions import DBException
from .DBBase import DBBase
from ..modelhelper import AppControlRol, AppControl

class ProfileDBHelper(DBBase):
    
    def ValidateProfile(self, profile:dict, ep_id:str)->bool:
        """
        Validate the user profile for EP.

        Args:
            profile (dict): User profile in DB.
            ep_id (str): EndPoint Id.

        Raises:
            DBException: SQLAlchemyError
            DBException: Exception

        Returns:
            bool: return True if the user have access to the EP.
        """
        session = self.getsession()
        try:
            if 'ADMINISTRATOR' in profile['access_level']:
                return True
            else:
                access = session.query(AppControlRol, AppControl).filter(AppControl.id==AppControlRol.id_appcontrol).\
                    filter(AppControl.app=='BACKEND').\
                    filter(AppControlRol.id_systemrol.in_(profile['access_level'])).\
                    filter(AppControlRol.id_appcontrol==str(ep_id)).all()

                if access:
                    return True
                else:
                    return False
        except Exception as oEx:
            self.log.error(str(oEx))
            raise DBException('1.1.7.1', 'ValidateProfile', 'Generic error validating profile: %s' % str(oEx))
        finally:
            self.closeSession()

