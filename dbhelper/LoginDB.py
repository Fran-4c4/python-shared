'''
- author: Francisco R. Moreno Santana franrms@gmail.com



'''
import logging
from model import User, Company

from gshared.exceptions.DBException import DBException
from .DBBase import DBBase
from .TokenDBHelper import TokenDBHelper
class LoginDB(DBBase):

    def __init__(self):
        super().__init__()
        self.log= logging.getLogger(__name__)


    def login(self,username:str, password:str)->dict:
        """
        Return user data.

        Args:
            username (str): username.
            password (str): user pass.

        Returns:
            dict: Dict with user data and token. Return False if the credentials are invalid.
        """       
        try:
            userToken = self.getUserTokenlogin(username, password)
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
            password (str): user password

        Raises:
            DBException: SQLAlchemyError
            DBException: Exception

        Returns:
            dict: Dict with user data and token.
        """
                     
        session = self.getsession()
        user = None
        try:
            user = session.query(User).filter(User.username == username, User.password == password, User.deleted==False).first()
            if user is None:
                raise DBException('1.0.2', 'login', 'Your credentials are not valid')        
        finally:
            pass

        try:

            companies = session.query(Company).filter(Company.id==user.id_company).all()
            _user = {
                'company': {
                    },
                'id': user.id,
                'username': user.username,
                'password': '**************',
                'name': user.name,
                'surname': user.surname,
                'language': getattr(user, 'language', "en"),
                'info': user.info,
                'profile': user.profile,
                }
            if len(companies)>0:
                _user['company']['id'] = companies[0].id
                _user['company']['name'] = companies[0].name   

            token_helper=TokenDBHelper(session)     
            token=token_helper.validate(user)
                            
            user_ret = _user
            #TODO: esta linea falla cuando se reinicia desde el modo suspension pero tenerlo en cuenta                         
            return {'user': user_ret, 'token': token.token}
        except Exception as oEx:
            raise DBException('1.0.2', 'login', 'Exception accessing DB doing login: %s' % str(oEx))
        finally:
            self.closeSession()
           