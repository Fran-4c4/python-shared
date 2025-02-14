'''
- author: Angel A. Velazquez angel.velazquez@geoaitech.com

'''

import logging
from sqlalchemy.exc import SQLAlchemyError
from model import User, Company
from gshared.exceptions.DBException import DBException
import GlobalConfig 
from .DBBase import DBBase
from .MapServerTokenDBHelper import MapServerTokenDBHelper

class MapServerLoginDB(DBBase):

    def __init__(self):
        super().__init__()
        self.log= logging.getLogger(__name__)
        self.global_config=GlobalConfig.app_config
        self.mapserverUser = self.global_config['map_server']['user']
        self.mapserverPass = self.global_config['map_server']['pass']


    def loginMapServer(self)->dict:
        """
        Return user data.

        Args:
            username (str): username.
            password (str): user pass.

        Returns:
            dict: Dict with user data and token. Return False if the credentials are invalid.
        """       
        try:
            userToken = self.getUserTokenlogin(self.mapserverUser, self.mapserverPass)
            if not userToken:
                return False
            return userToken
        except Exception as oEx:
            self.log.error(oEx)
        return False

    def getUserTokenlogin(self, username:str, password:str)->dict:
        """
        Return user data.

        Args:
            username (str): username
            password (str): user pass

        Raises:
            DBException: SQLAlchemyError
            DBException: Exception

        Returns:
            dict: Dict with user data and token.
        """
                     
        session = self.getsession()
        user = None
        try:
            user = session.query(User).filter(User.username == username, User.password == password).one()
            companies = session.query(Company).filter(Company.id==user.id_company).all()
            _user = {
                'company': {
                    },
                'id': user.id,
                'username': user.username,
                'password': '**************',
                'name': user.name,
                'surname': user.surname,
                'info': user.info,
                'profile': user.profile,
                }
            if len(companies)>0:
                _user['company']['id'] = companies[0].id
                _user['company']['name'] = companies[0].name   

            token_helper=MapServerTokenDBHelper(session)     
            token=token_helper.getMapServerToken(user)
                            
            user_ret = _user
            #TODO: esta linea falla cuando se reinicia desde el modo suspension pero tenerlo en cuenta
            self.Log(user_ret['id'], 'info', 'Login', user_ret)                
            return {'user': user_ret, 'token': token.token}
        except SQLAlchemyError as oEx:
            session.rollback()
            self.Log(None, 'error', 'Login', {'username': username, 'error': str(oEx)})
            raise DBException('1.0.1', 'login', 'Exception accessing DB doing login: %s' % str(oEx.args))
        except Exception as oEx:
            session.rollback()
            self.Log(None, 'error', 'Login', {'username': username,  'error': str(oEx)})
            raise DBException('1.0.2', 'login', 'Exception accessing DB doing login: %s' % str(oEx))
        finally:
            self.closeSession()
           