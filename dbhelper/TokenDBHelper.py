'''
- author: Francisco R. Moreno Santana franrms@gmail.com



'''
import sys
import platform

from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import datetime as dt

import jwt
from model import UserToken, User
from sqlalchemy.orm.session import Session
from gshared.exceptions  import  DBException
from .DBBase import DBBase


class TokenDBHelper(DBBase):

    def __init__(self,scoped_session=None):
        super().__init__(scoped_session)
        import GlobalConfig #to avoid circular dependency
        self.global_config=GlobalConfig.app_config
        self.secret_key = self.global_config['token']['secret_key']
        self.algorithm = self.global_config['token']['algorithm']

    def validate(self, user:User, scoped_session:Session=None)->UserToken:
        """


        Args:
            user (User): User object.
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
            "exp": tokenExpirationDateTimeStamp
            }

            token_jwt=None
            #jwt return bytes using docs, but in some cases is string so we can not decode
            token_bytes=jwt.encode(token_payload, key, algorithm=algorithm)
            if isinstance(token_bytes, (bytes, bytearray)):
                token_jwt = token_bytes.decode('UTF-8')
            else:
                token_jwt = token_bytes


            token = session.query(UserToken).filter(UserToken.id_user==user.id).first()

            if token is None:  
                try:
                    token = UserToken(id_user=user.id, token=token_jwt, created=token_created, expires=tokenExpirationDate)
                    session.add(token)                
                    session.commit()
                except IntegrityError as oEx:
                    session.rollback()
                    self.log.error('Integrity Error creating new Token for user ' + user.id )
                except Exception as oEx:
                    session.rollback()
                    self.log.error('Error creating new Token for user ' + user.id )
                    
            else:
                if token.expires < tokenExpirationDate:   
                    try:                   
                        token.expires = tokenExpirationDate
                        token.token = str(token_jwt)
                        token.created = token_created
                    
                        session.commit()
                    except Exception as oEx:
                        self.log.error('Error Updating token for user' + user.id)
                        session.rollback()
            
            return token
        except Exception as oEx:
            session.rollback()                
            raise DBException('1.0.2', 'login', 'Exception validate token: %s' % str(oEx),oEx)
        finally:
            self.closeSession()

    def ValidateToken(self, token:dict)->str:
        """
        Checks if the user token is valid.

        Args:
            token (dict): Token info.

        Raises:
            DBException: SQLAlchemyError
            DBException: Exception

        Returns:
            str: User token.
        """
        try:
            id_user=token['id_user']
            expDate=token['exp']
            now=dt.datetime.utcnow()            
            tokenExpirationDate =dt.datetime.fromtimestamp(expDate)
            if tokenExpirationDate<now:
                return None
            token = self.GetTokenForUserId(id_user)
            if token is not None:
                return token['token']
            return None

        except Exception as oEx:
            self.log.error(str(oEx))
            raise DBException('1.1.7.1', 'ValidatingToken', 'Generic error validating token: %s' % str(oEx))
        finally:
            self.closeSession()

    def GetTokenForUserId(self, id_user:str)->dict:
        """
        Return the token of a specific user.

        Args:
            id_user (str): User id.

        Raises:
            DBException: SQLAlchemyError
            DBException: Exception

        Returns:
            dict: Uer token data.
        """      
        session = self.getsession()
        _token = None
        try:
            _token = session.query(UserToken).filter(UserToken.id_user==id_user).first()
            if _token:
                _token = self.row2dict(_token)
            return _token
        except SQLAlchemyError as oEx:
            self.log.error(oEx)
            raise DBException('1.1.5.0', 'GetTokenForUsername', 'Error finding the token for user id: %s' % str(oEx.args))
        except Exception as oEx:
            self.log.error(oEx)
            raise DBException('1.1.5.1', 'GetTokenForUsername', 'Generic Error finding the token for user id: %s' % str(oEx))
        finally:
            self.closeSession()