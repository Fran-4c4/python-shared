import os
import json
from typing import Dict
import warnings
from pathlib import Path

from sqlalchemy import text as sqltext
from .AppConfigEnum import AppConfigEnum
from gshared.dbhelper import DBBase, ErrorDB
from gshared.sqlalchemyhelper import Row2Dict

#APP_ENV = os.environ.get("APP_ENV") or "local"  # or 'live' to load live
class ConfigurationHelper():
    """Basic class to load configuration for application    
    """    
    config=None

    def loadConfig(self,config_file_name:str,rootPath:str=None)->dict:
        """ 

        Reads the json file with the app config and return a dict with the configuration.

        Args:
            config_file_name (str): _description_
            rootPath (str): _description_

        Returns:
            dict: configuration
        """     
        warnings.warn(
        "loadConfig() is deprecated and will be removed in future versions. Use load_config_like() instead.",
        DeprecationWarning,
        stacklevel=2
    )
        f = open(os.path.join(rootPath, config_file_name))
        cfg = json.load(f)
        f.close()    
        self.config=cfg

        keys=cfg.keys()
        for k in keys:
            self.updatefieldWithEnvironment(k)

        #special cases
        self.updatefieldWithEnvironment("DDBB_CONFIG","db")
        self.updatefieldWithEnvironment("DDBB_CONFIG","db_log")            

        return cfg
    
    def load_config_like(self,config_like:str)->dict:
        """ 

        get app config from file or dict and return a dict with the configuration.

        Args:
            config_file_name (str): full file name or dict
        Returns:
            dict: configuration
        """     
   
        cfg = self.load_parse_cfg_file(config_like=config_like)  
        self.config=cfg

        keys=cfg.keys()
        for k in keys:
            self.updatefieldWithEnvironment(k)

        #special cases
        self.updatefieldWithEnvironment("DDBB_CONFIG","db")
        self.updatefieldWithEnvironment("DDBB_CONFIG","db_log")            

        return cfg
    
    def load_parse_cfg_file(self,config_like):
        config_data=None
        if isinstance(config_like, str) and os.path.exists(config_like):
            config_data = ConfigurationHelper().load_file(file_path=config_like)               
        elif isinstance(config_like, dict):
            config_data = config_like
        else:
            raise ValueError("config_like must be path to config file or JSON object.")
        
        return config_data
    
    def load_file(self, file_path):
        """
        load JSON from file path.
        """
        with open(file_path, 'r', encoding='utf-8') as mfile:
            return json.load(mfile)
    
    def loadConfigFromDDBB(self,config:Dict)->dict:
        """load config from database and assign to config object

        Args:
            config (Dict): source configuration

        Returns:
            dict: configuration
        """        
        if config is None:
            config={}

        sSQL = F"""
                    select * 
                    from configuration c 
                    where c.config_for in ('SERVER','ALL');
                    """            


        db = DBBase()            
        session = db.getsession()
        queryList = session.execute(sqltext(sSQL)).all() 
        for r in queryList:
            config[r.key]=r.value #replace config with server configuration
            #Now we need to map server keys with config file keys
            if r.key==AppConfigEnum.AUTH_CONFIG:
                config[AppConfigEnum.CFG_AUTH_CONFIG]=r.value
            if r.key==AppConfigEnum.REPOSITORY_CONFIG:
                config[AppConfigEnum.CFG_REPOSITORY]=r.value                
            elif r.key==AppConfigEnum.QUEUE_NOTIFICATIONS_CONFIG:
                config[AppConfigEnum.CFG_QUEUE_NOTIFICATIONS_CONFIG]=r.value
            elif r.key==AppConfigEnum.QUEUE_EMAIL_CONFIG:
                config[AppConfigEnum.CFG_QUEUE_EMAIL_CONFIG]=r.value  
            elif r.key==AppConfigEnum.FARGATE_TASKS_CONFIG:
                config[AppConfigEnum.CFG_FARGATE_TASKS_CONFIG]=r.value                            

        


        return config

    def updatefieldWithEnvironment(self,env_key, config_key=None):
        if config_key is None:
            config_key=env_key #cuando se llaman igual    
        new_value = os.environ.get(env_key)
        if new_value is not None:	
            try:
                new_value_dict = json.loads(new_value)
                new_value=new_value_dict
            except Exception as oEx:
                pass #the value is string
                
            self.config[config_key]=new_value
        
    
    def loadErrors(self)->dict:
        """
        Gets all the error messages in DB and return a dict.

        Returns:
            dict: A dict with the error messages.
        """
        error_list = ErrorDB().list()
        errors={}
        for row in error_list:
            err = Row2Dict.row2dict(row)
            errors[err['id']]=err
        return errors


    def load_env(self,app_name,env_name=None) -> None:
        from dotenv import load_dotenv
        # Get the user's home directory
        home_dir = Path(os.path.expanduser('~'))

        # Get the environment from the ENVIRONMENT variable, default to 'development'
        environment = os.getenv('ENVIRONMENT',env_name )
        print(f"Environment is {environment}")    

        # Build the path to the environment file (e.g., .env.development or .env.production)
        fname=f'.env.{app_name}'
        if environment:
            fname+="." + environment
        env_file = home_dir / '.envs' / fname

        # Load the environment file
        load_dotenv(dotenv_path=env_file)

        # Now you can access environment variables
        # DDBB_CONFIG = os.getenv('DDBB_CONFIG') 
        print("Environment vars loaded") 