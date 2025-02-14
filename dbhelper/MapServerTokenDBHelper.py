'''
- author: Angel A. Velazquez angel.velazquez@geoaitech.com


'''
import sys
import platform
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import datetime as dt
import jwt
from model import UserToken, User
from sqlalchemy.orm.session import Session
from gshared.exceptions import  DBException
from .DBBase import DBBase
import GlobalConfig 

class MapServerTokenDBHelper(DBBase):

    def __init__(self,scoped_session=None):
        super().__init__(scoped_session)
        self.global_config=GlobalConfig.app_config
        self.secret_key = self.global_config['token']['secret_key']
        self.algorithm = self.global_config['token']['algorithm']
        self.service_name = self.global_config['map_server']['service_name']

    def getMapServerToken(self, user:User, scoped_session:Session=None)->UserToken:
        """
        Returns the user token to use mapserver.


        Args:
            user (User): User object.
            service (str): Service that will make use of the map server 
            scoped_session (Session, optional): DB Session. Defaults to None.

        Raises:
            DBException: SQLAlchemyError
            DBException: Exception

        Returns:
            UserToken: UserToken object with the token data.
        """      
        session = self.getsession()
        token = None
        try: 
            key=self.secret_key
            algorithm=self.algorithm
            token_created=dt.datetime.utcnow()
            tokenExpirationDate=token_created+dt.timedelta(seconds=(86400*7)) # token de 7 días
            tokenExpirationDateTimeStamp =dt.datetime.timestamp(tokenExpirationDate)

            token_payload = {
            "id_user": user.id,
            "service": self.service_name,
            "exp": tokenExpirationDateTimeStamp
            }

            if "3.7" in sys.version or "3.8" in sys.version:
                if platform.system()=='Windows': 
                    token_jwt = jwt.encode(token_payload, key, algorithm=algorithm).decode('UTF-8')
                else:
                    token_jwt = jwt.encode(token_payload, key, algorithm=algorithm).decode('UTF-8')
            else:
                token_jwt = jwt.encode(token_payload, key, algorithm=algorithm)

            try:
                token = session.query(UserToken).filter(UserToken.id_user==user.id).first()
            except Exception:
                self.log.error('Token for user [%s] doesn\'t exists. Creating a new one...' % user.id)
                
            if token is None:  
                try:
                    token = UserToken(id_user=user.id, token=token_jwt, created=token_created, expires=tokenExpirationDate)
                    session.add(token)                
                    session.commit()
                except IntegrityError as oEx:
                    session.rollback()
                    self.log.error('Integrity Error creating new Token for mapserver user ' + user.id )
                except Exception as oEx:
                    session.rollback()
                    self.log.error('Error creating new Token for mapserver user ' + user.id )
                    
            else: 
                try:                   
                    token.expires = tokenExpirationDate
                    token.token = str(token_jwt)
                    token.created = token_created
                
                    session.commit()
                except Exception as oEx:
                    self.log.error('Error Updating token for mapserver user' + user.id)
                    session.rollback()
            
            return token
        except SQLAlchemyError as oEx:
            session.rollback()
            raise DBException('1.0.1', 'login', 'Exception validate token for mapserver: %s' % str(oEx.args))
        except Exception as oEx:
            session.rollback()                
            raise DBException('1.0.2', 'login', 'Exception validate token for mapserver: %s' % str(oEx))
        finally:
            self.closeSession()