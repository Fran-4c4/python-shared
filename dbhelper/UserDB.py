'''
- author: Francisco R. Moreno Santana franrms@gmail.com
'''
import json
import logging
import jwt
import hashlib

from gshared.tools.Warnings import deprecated

from sqlalchemy.exc import SQLAlchemyError
import datetime as dt
from gshared.configurations import AppConfigEnum


from gshared.modelhelper import AuthConfig, Configuration
from gshared.queue import QueueFactory

from model import User, UserToken, Company

from gshared.exceptions import  DBException
from .DBBase import DBBase
from sqlalchemy.sql import text as sqltext

#import db.DBE as DBE
#from db import DBE as DBE
#from db import DBE 
#import db.DBE as DBE
from .DBHErrors import DBHErrors as DBE
class UserDB(DBBase):

    def __init__(self,scoped_session=None):
        super().__init__(scoped_session)
        self.log= logging.getLogger(__name__)


    def getUserByToken(self, token:str)->User:
        """
        Returns a specific user by a Token.

        Args:
            token (str): User token.

        Raises:
            DBException: SQLAlchemyError
            DBException: Exception

        Returns:
            User: User
        """
        session = self.getsession()
        try:
            result = session.query(User, UserToken).filter(
                User.id == UserToken.id_user).filter(UserToken.token == token).one()
            return result.User
        except SQLAlchemyError as oEx:
            self.log.error(str(oEx))
            raise DBException('1.1.6.0', 'GetUserForToken',
                              'Error retrieving a user for token %s' % str(oEx.args))
        except Exception as oEx:
            self.log.error(str(oEx))
            raise DBException('1.1.6.1', 'GetUserForToken',
                              'Generic Error retrieving a user for token: %s' % str(oEx))

    def get(self, id:str, filter:dict=None)->dict:
        """
        Return an user object + company based on id.
        this method is used internally, please do not use with api endpoints use get_user_full instead

        Args:
            id (str): User id.
            filter (dict, optional): Filters. Defaults to None.

        Raises:
            DBException: Exception

        Returns:
            dict: user object + company.
        """
        session = self.getsession()
        try:
            res = session.query(User, Company). \
                filter(User.id_company == Company.id). \
                filter(User.id == str(id), User.deleted !=
                       True).one()  # user must exist
            self.closeSession()
            _user = res.User
            _company = res.Company

            userRet = {
                'company': {
                },
                'id': _user.id,
                'username': _user.username,
                'password': '**************',
                'name': _user.name,
                'surname': _user.surname,
                'language': getattr(_user, 'language', "en"),
                'info': _user.info,
                'profile': _user.profile,
            }

            userRet['company']['id'] = _company.id
            userRet['company']['name'] = _company.name

            if userRet['info'] == None:
                userRet['info'] = {}

            return userRet
        
        except Exception as oEx:
            self.log.error(str(oEx))
            raise DBException('1.1.0.1', 'GetUser',
                              'Generic retrieving user from id: %s' % str(oEx))
        finally:
            self.closeSession()
            
    def get_user_basic_data(self, id:str)->dict:
        """
        Return an user object 

        Args:
            id (str): User id.
        Raises:
            DBException: Exception
        Returns:
            dict: user object 
        """
        session = self.getsession()
        try:
            _user:User = session.query(User). \
                filter(User.id == str(id), User.deleted !=
                       True).one()  # user must exist
            self.closeSession()

            userRet = {
                'id': str(_user.id),
                'username': _user.username,
                'name': _user.name,
                'surname': _user.surname,
                'language': getattr(_user, 'language', "en"),
                'profile': _user.profile,
                'id_company': str(_user.id_company)
            }
            
            return userRet        
        except Exception as oEx:
            self.log.error(str(oEx))
            raise DBException('1.1.0.1', 'GetUser',
                              'Generic retrieving user from id: %s' % str(oEx))
        finally:
            self.closeSession()

    def get_user_full(self, id_user:str, id_company:str)->dict:
        """
        Return an user object 
        Args:
            id (str): User id.
        Raises:
            DBException: DBException

        Returns:
            dict: user object .
        """
        session = self.getsession()
        try:
            res = session.query(User). \
                filter(User.id_company == str(id_company)). \
                filter(User.id == str(id_user)).first()  
            return res

        except Exception as oEx:
            raise DBException('1.1.0.1', 'GetUser',
                              'Generic retrieving user from id: %s' % str(oEx),oEx)
        finally:
            self.closeSession()

    @deprecated(message=" USE update_v2 instead")
    def update(self, ruser:dict, user2Modify:dict)->dict :
        """DEPRECATED USE update_v2 instead
        Update or add a User. If the User object contains de field 'id', update the existed row. Otherwise create a new one.

        Args:
            ruser (dict): User that make the operation.
            user2Modify (dict): User data

        Returns:
            dict: User added/modified
        """
        if 'id' in user2Modify:
            return self.modify(ruser, user2Modify)
        else:
            return self.add(ruser, user2Modify)
        
    def update_v2(self, ruser:dict, user2Modify:dict)->dict :
        """Update or add a User. If the User object contains de field 'id', update the existed row. Otherwise create a new one.

        Args:
            ruser (dict): User that make the operation.
            user2Modify (dict): User data

        Returns:
            dict: User added/modified
        """
        if 'id' in user2Modify:
            return self.modify_v2(ruser, user2Modify)
        else:
            return self.add_v2(ruser, user2Modify)        

    def add_v2(self, ruser:dict, user2Modify:dict)->dict:
        """ADD a new user.

        Args:
            ruser (dict): User that make the operation.
            user2Modify (dict): user data.

        Raises:
            DBException: SQLAlchemyError exception.
            DBException: Exception exception.

        Returns:
            dict: User added
        """
        session = self.getsession()
        try:
            if 'username' not in user2Modify:
                raise DBException('1.1.1.2', 'Add User', 'An "username" is mandatory')
            if 'password' not in user2Modify:
                raise DBException('1.1.1.3', 'Add User', 'A "password" is mandatory')
            if 'name' not in user2Modify:
                raise DBException('1.1.1.4', 'Add User','A "name" is mandatory')
            if 'surname' not in user2Modify:
                raise DBException('1.1.1.5', 'Add User','A "surname" is mandatory')
            if 'id_company' not in user2Modify:
                raise DBException('1.1.1.6', 'Add User','An "id_company" is mandatory')
            if 'profile' not in user2Modify:
                user2Modify = {}
            if 'info' not in user2Modify:
                user2Modify['info'] = {}
            else:
                info = user2Modify['info']


            modification_date = self.getServerDateTimeWithZoneString()  
            self.insertCreationTime(ruser=ruser,item2Update=user2Modify,updating_date=modification_date)
            __user = User(name=user2Modify['name'],
                          surname=user2Modify['surname'],
                          username=user2Modify['username'],
                          password=user2Modify['password'],
                          id_company=user2Modify['id_company'],
                          profile=user2Modify['profile'],
                          info=info)
            session.add(__user)

            if self.scoped_session is None:
                #commit must be done in caller. 
                # Because there are some projects expecting the commit here we maintain this only when the session is own
                session.commit()
            return __user
        except Exception as oEx:
            raise DBException(code=DBE.ERR_1111,exception= oEx,logger=self.log)
        finally:
            self.closeSession()



    def add(self, ruser:dict, user2Modify:dict)->dict:
        """ADD a new user.

        Args:
            ruser (dict): User that make the operation.
            user2Modify (dict): user data.

        Raises:
            DBException: SQLAlchemyError exception.
            DBException: Exception exception.

        Returns:
            dict: User added
        """
        session = self.getsession()
        try:
            if 'username' not in user2Modify:
                raise DBException('1.1.1.2', 'Add User', 'An "username" is mandatory')
            if 'password' not in user2Modify:
                raise DBException('1.1.1.3', 'Add User', 'A "password" is mandatory')
            if 'name' not in user2Modify:
                raise DBException('1.1.1.4', 'Add User','A "name" is mandatory')
            if 'surname' not in user2Modify:
                raise DBException('1.1.1.5', 'Add User','A "surname" is mandatory')
            if 'id_company' not in user2Modify:
                raise DBException('1.1.1.6', 'Add User','An "id_company" is mandatory')
            if 'profile' not in user2Modify:
                user2Modify = {}
            if 'info' not in user2Modify:
                user2Modify['info'] = {}
            else:
                info = user2Modify['info']


            modification_date = self.getServerDateTimeWithZoneString()  
            self.insertCreationTime(ruser=ruser,item2Update=user2Modify,updating_date=modification_date)
            __user = User(name=user2Modify['name'],
                          surname=user2Modify['surname'],
                          username=user2Modify['username'],
                          password=user2Modify['password'],
                          id_company=user2Modify['id_company'],
                          profile=user2Modify['profile'],
                          info=info)
            session.add(__user)

            if self.scoped_session is None:
                #commit must be done in caller. 
                # Because there are some projects expecting the commit here we maintain this only when the session is own
                session.commit()

            user_ret = self.row2dict(__user)
            return user_ret
        except Exception as oEx:
            raise DBException(code=DBE.ERR_1111,exception= oEx,logger=self.log)
        finally:
            self.closeSession()


    @deprecated(" USE modify_v2 instead")
    def modify(self, ruser:dict, user2Modify:dict)->dict:
        """Modify user, DEPRECATED USE modify_v2 instead

        Args:
            ruser (dict): User that make the operation.
            user2Modify (dict): user data

        Raises:
            DBException: SQLAlchemyError exception.
            DBException: Exception exception.

        Returns:
            dict: user modified
        """
        session = self.getsession()
        try:
            user_obj = self.modify_v2(ruser=ruser,user2Modify=user2Modify)

            user_ret = self.row2dict(user_obj)
            session.commit()

            return user_ret
        except SQLAlchemyError as oEx:
            session.rollback()
            self.Log(ruser['id'], 'error', 'ModifyUser', {
                     'object': user2Modify, 'error': str(oEx)})
            raise DBException('1.1.2.0', 'ModifyUser',
                              'Error modifying a user: %s' % str(oEx.args))
        except Exception as oEx:
            session.rollback()
            self.Log(ruser['id'], 'error', 'ModifyUser', {
                     'object': user2Modify, 'error': str(oEx)})
            raise DBException('1.1.2.1', 'ModifyUser',
                              'Generic error modifying a user: %s' % str(oEx))
        finally:
            self.closeSession()


    def modify_v2(self, ruser:dict, user2Modify:dict)->User:
        """Modifies a User.

        Args:
            ruser (dict): User that make the operation.
            user2Modify (dict): user data

        Raises:
            DBException: SQLAlchemyError exception.
            DBException: Exception exception.

        Returns:
            dict: user modified
        """
        session = self.getsession()
        try:
            user_obj = session.query(User).filter(
                User.id == user2Modify['id']).one()

            if user_obj.info is None or user_obj.info == '':
                user_obj.info = {}
            if user_obj.profile is None or user_obj.profile == '':
                user_obj.profile = {}
            if 'info' in user2Modify:
                for k in user2Modify['info']:
                    user_obj.info[k] = user2Modify['info'][k]

            self.updateModifyTime(ruser= ruser,item2Update=user_obj)

            if 'id_company' in user2Modify:
                user_obj.id_company = user2Modify['id_company']
            if 'username' in user2Modify:
                user_obj.username = user2Modify['username']
            if 'password' in user2Modify:
                user_obj.password = user2Modify['password']
            if 'name' in user2Modify:
                user_obj.name = user2Modify['name']
            if 'surname' in user2Modify:
                user_obj.surname = user2Modify['surname']
            if user_obj.profile is None or user_obj.profile == '':
                user_obj.profile = {}
            if 'profile' in user2Modify:
                for k in user2Modify['profile']:
                    user_obj.profile[k] = user2Modify['profile'][k]
                self.markJsonAsModified(user_obj,"profile")

            if 'username' in user2Modify and user_obj.username != user2Modify['username']:
                tok = session.query(UserToken).filter(
                    UserToken.id_user == user2Modify['id']).one()
                session.delete(tok)

            self.Log(ruser['id'], 'info', 'ModifyUser', {
                     'object': self.row2dict(user_obj)})
            return user_obj
        except Exception as oEx:
            self.Log(ruser['id'], 'error', 'ModifyUser', {
                     'object': user2Modify, 'error': str(oEx)})
            raise DBException('1.1.2.1', 'ModifyUser',
                              'Error modifying user: %s' % str(oEx),oEx)
        finally:
            self.closeSession()


    def delete(self, user:dict, id_user:str):
        """Delete a specific User.

        Args:
            user (dict): User data
            id2Delete (str): User id

        Raises:
            DBException: SQLAlchemyError exception.
            DBException: Exception exception.
        """
        session = self.getsession()
        try:
            _user = session.query(User).get(id_user)
            session.delete(_user)
            session.commit()

            self.Log(user['id'], 'info', 'DeleteUser',
                     {'object': {'id_user': id_user}})

        except Exception as oEx:
            session.rollback()
            self.Log(user['id'], 'error', 'DeleteUser', {
                     'object': {'id_user': id_user}, 'error': str(oEx)})
            raise DBException('1.1.3.1', 'DeleteUser',
                              'Error : %s' % str(oEx),oEx)
        finally:
            self.closeSession()



    def list(self, id_company:str, filter=None):
        """List users of company

        Args:
            id_company (str): company id
            filter (_type_, optional): _description_. Defaults to None.

        Raises:
            DBException: _description_
        Returns:
            _type_: _description_
        """        
        session = self.getsession()
        try:
            listUsers = []

            if filter is None:
                users = session.query(User, Company).\
                    filter(User.id_company == Company.id).\
                    filter(Company.id == id_company).all()
                self.closeSession()  # close session as soon of possible

                for _row in users:
                    _user = _row.User
                    _company = _row.Company

                    u = {
                        'company': {
                            'id': str(_company.id),
                            'name': _company.name
                        },
                        'id': str(_user.id),
                        'username': _user.username,
                        'password': '**************',
                        'name': _user.name,
                        'surname': _user.surname,
                        'info': _user.info,
                        'profile': _user.profile,
                    }
                    listUsers.append(u)
            else:
                sSELECT = 'select u.id,u.username , u."name" ,u.surname , u.profile , u.info , u.id_company  , c."name" as company_name '
                sFROM = ' from public.user u inner join company c on c.id = u.id_company '
                sWHERE = ' WHERE u.id_company=:id_company '
                params={}
                params["id_company"]=str(id_company)
                sql = self.getFilter(filter, sSELECT, sFROM, sWHERE)
                users = session.execute(sqltext(sql),params=params).mappings().all()
                for u in users:
                    u = {
                        'company': {
                            'id': str(u.get("id_company")),
                            'name': u.get("company_name")
                        },
                        'id': str(u.get("id")),
                        'username': u.get("username"),
                        'password': '**************',
                        'name': u.get("name"),
                        'surname': u.get("surname"),
                        'info': u.get("info"),
                        'profile': u.get("profile")
                    }
                    listUsers.append(u)
                    
            return listUsers
        except Exception as oEx:
            self.log.error(str(oEx))
            raise DBException(
                '1.1.4.1', 'list', 'Generic Error retrieving list of users: %s' % str(oEx))
        finally:
            self.closeSession()

    def delete_project_relation(self, user:dict, id_user:str, id_project:str):
        """
        Remove the relation user-project.

        Args:
            user (dict): User that make the operation.
            id_user (str): User id.
            id_project (str): Project id.

        Raises:
            DBException: SQLAlchemyError exception.
        """
        #todo: esto se ha colado aqui, creo que pertenece al labeler
        session = self.getsession()
        try:           
            sql = "DELETE FROM user_project where  id_user='%s'::uuid AND id_project='%s'::uuid" % (str(id_user) , id_project)
            session.execute(sqltext(sql))
            session.commit()

        except SQLAlchemyError as oEx:
            session.rollback()
            self.Log(user['id'], 'error', 'delete_project_relation', {
                     'object': {'id_user': id_user}, 'error': str(oEx)})
            raise DBException('1.1.1.1', 'delete_project_relation','Database error: %s' % str(oEx.args))
        except Exception as oEx:
            session.rollback()
            self.Log(user['id'], 'error', 'delete_project_relation', {
                     'object': {'id_user': id_user}, 'error': str(oEx)})
            raise DBException('1.1.1.1', 'delete_project_relation','Generic error: %s' % str(oEx))
        finally:
            self.closeSession()

   


    def reset_password_by_email(self, username:str):
        """Reset user password by email. Send an email to queue
            flow: DO NOT REMOVE OLD PASSWORD UNTIL RESET
            1-create reset token_password in user info object for example with expiration 4 hours for example
            2-send email to user
            3-user open link (in main app or in new webpage) , check token expiration, if correct change password
            4-send email with change, this is useful to allow the user if someone changed his password without his knowledge
        Args:
            username (str): email of user

        Raises:
            DBException: exception
        """        
        """ 

         """
        #get user by email
        session=self.getsession()
        user:User = session.query(User). \
                filter(User.username == str(username), User.deleted !=
                       True).first()  # user must exist
        if user is None:
            raise DBException(DBE.ERR_112)
        
        _srv_config_entry=session.query(Configuration). \
                filter(Configuration.key == AppConfigEnum.AUTH_CONFIG).one()  # data must exist 
        

        value_dict=self.convertRowJsonToDict(_srv_config_entry.value)
        authConfig=AuthConfig(value_dict)   

        _srv_config_entry=session.query(Configuration). \
                filter(Configuration.key == AppConfigEnum.RESET_EMAIL_CONFIG).one()  # data must exist 
        reset_email_config=_srv_config_entry.value 

        _srv_config_entry=session.query(Configuration). \
                filter(Configuration.key == AppConfigEnum.QUEUE_EMAIL_CONFIG).one()  # data must exist 
        _queue_email_config =_srv_config_entry.value  

        #create token
        token=self.generate_password_reset_token(username=user.username,authConfig=authConfig)
        #.decode('UTF-8')
        #send email to user... directly or using a queue
        language="EN-EN"
             
        msg=reset_email_config.get("reset_email_html_template",msg) #message template is in DDBBB  
        msg=msg.format(reset_email_url=reset_email_config["reset_email_url"],user_name=(user.name + " " + user.surname), id=username,token=token ,language=language )
        qmsg={}
        qmsg["sendto"]=user.username #email
        qmsg["subject"]="Reset password"
        qmsg["body_html"]=msg
        jmsg=json.dumps(qmsg)
        #send to queue
        _queue=QueueFactory(_queue_email_config).tool
        _queue.send_message(jmsg)

    def generate_password_reset_token(self,username:str,authConfig:AuthConfig)->str:
        """generate a token with info for reseting
            {
                "exp": exp,
                "nbf": now,
                "sub": username,
                "username": username,
            },  
        Args:
            username (str): user name, in fact an email
            authConfig (Authconfig): auth configuration object for token decription

        Returns:
            str: encoded token
        """        
        delta = dt.timedelta(hours=authConfig.email_reset_token_expire_hours)
        now = dt.datetime.utcnow()
        expires = now + delta
        exp = expires.timestamp()
        encoded_jwt = jwt.encode(
            {
                "exp": exp,
                "nbf": now,
                "sub": username,
                "username": username,
            },
            authConfig.secret_key,
            algorithm=authConfig.algorithm,
        )
        return encoded_jwt 

        

    def change_password(self, user_name: str, token: str, password: str, passwordConfirm: str,authConfig:AuthConfig)->User:
            """change user password by reset

            Args:
                user_name (str): user name, in fact an email
                token (str): token used in request change
                password (str): new password
                passwordConfirm (str): password Confirm. 
                authConfig (Authconfig): auth configuration object for token decription

            Returns:
                User: user object modified
            """        
            session=self.getsession()
            user:User = session.query(User). \
                    filter(User.username == str(user_name), User.deleted !=
                        True).first()  # user must exist
            if user is None:
                raise DBException(DBE.ERR_112)
            
            #read token
            tokenDecoded=None
            try:
                tokenDecoded=jwt.decode(token, authConfig.secret_key,algorithms=[authConfig.algorithm])
            except jwt.ExpiredSignatureError:
                # Signature has expired
                raise DBException(DBE.ERR_113, message= "Token expired, please request again")

            t_user_name=tokenDecoded['username']
            if t_user_name!=user_name:
                raise DBException(DBE.ERR_113, message= "Invalid token and username")
            if password != passwordConfirm:
                    raise DBException(DBE.ERR_113, message= "Password do not match")
            
            md5 = hashlib.md5()
            md5.update(str.encode(password))
            md5.digest()
            print(md5.hexdigest())
            user.password=md5.hexdigest()
            return user
            #session.commit() do in caller
        
    
    def change_password_by_app(self,ruser , id_user: str, new_password: str)->User:
        """change user password using UI interface

        Args:
            ruser: request user object, can be the user or an admin user
            id_user(str): id user 
            new_password (str): new password
            authConfig (Authconfig): auth configuration object for token decription

        Returns:
            User: user object modified
        """        
        session=self.getsession()
        user:User = session.query(User). \
                filter(User.id == str(id_user)).first()  # user must exist
        if user is None:
            raise DBException(DBE.ERR_112)

        user.password=new_password
        self.updateModifyTime(ruser= ruser,item2Update=user)
        return user
        #session.commit() do in caller



