
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
'''
- author: Francisco R. Moreno Santana franrms@gmail.com



'''
import datetime as dt
from .DBBase import DBBase
from gshared.modelhelper import Base, BaseLog
import urllib.parse


def initDatabase(cfg:dict):
    """
    Creates the connection with the database.

    Args:
        cfg (dict): Configuration data with the information of the connection to the db.

    Raises:
        oEx: SQLAlchemyError.
    """
    log = logging.getLogger(__name__)
    user=urllib.parse.quote( cfg['db']['user'])
    
    password=urllib.parse.quote(cfg['db']['password'])
    sCon ='postgresql://%s:%s@%s:%d/%s' % (user, password, cfg['db']['host'], cfg['db']['port'], cfg['db']['db'])
    sConLog = 'postgresql://%s:%s@%s:%d/%s' % (user, password, cfg['db_log']['host'], cfg['db_log']['port'], cfg['db_log']['db'])
    try:
        DBBase.gDbEngine = create_engine(sCon, pool_size=200, max_overflow=5)
        DBBase.gDbEngineLog = create_engine(sConLog, pool_size=200, max_overflow=5)
    except SQLAlchemyError as oEx:
        log.exception(oEx)
        raise oEx
        
    if DBBase.gDbEngine is None or DBBase.gDbEngineLog is None:
        log.error('Couldn\'t initialize connection to DB')
        DBBase.gDbEngine = None
        DBBase.gDbEngineLog = None
    else:
        Base.metadata.bind = DBBase.gDbEngine
        dbsession = sessionmaker(bind=DBBase.gDbEngine, autoflush=True)
        DBBase.gDBSession = dbsession
        BaseLog.metadata.bind = DBBase.gDbEngineLog
        dbsessionLog = sessionmaker(bind=DBBase.gDbEngineLog, autoflush=True)
        DBBase.gDBSessionLog = dbsessionLog()
