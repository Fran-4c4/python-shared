import hashlib
import inspect
import logging
import datetime as dt
from datetime import datetime
import json
import os
from typing import Any, Dict
from pytest import param
from sqlalchemy import func
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
#
from sqlalchemy.orm.session import Session
from geoalchemy2.functions import ST_SetSRID

from gshared.exceptions.DBException import DBException
from gshared.filetool import FileTool
from gshared.modelhelper import BaseLog, LogEntry
from gshared.sqlalchemyhelper import Row2Dict
from gshared.tools import DateHelper



class DBBase(object):
    """Base class for database access

    Args:
        object (object): object

    Returns:
        DBBase: DBBase
    """
    # global session shared between instances
    gDbEngine = None
    gDBSession = None
    gDbEngineLog = None
    gDBSessionLog = None
    session = None
    scoped_session = None
    log=None

    def __init__(self, scoped_session: Session = None):
        """init

        Args:
            scoped_session (Session, optional): scoped session is the main session used by methods and classes. You must use the same session for all operations included in a trasaction Defaults to None.
        """
        self.log: logging.Logger = logging.getLogger(__name__)
        if scoped_session:
            self.scoped_session = scoped_session

    # Calling destructor
    def __del__(self):
        """
        Closes the existing session.
        """
        # print(__name__ + "DBBase Destructor called")
        try:
            self.closeSession()
        except Exception as oEx:
            print("DBBase" + str(oEx))

    def closeSession(self):
        """
        Closes the existing session if it is exist.
        Do not close the scoped session
        """
        if self.session:
            self.session.close()
            return
        
    def commit_own(self):
        """
        If a scoped_session exist do not make a commit 

        Returns:
            _type_: Commit return.
        """
        if self.scoped_session:
            pass #no commit
        elif self.session:
            return self.session.commit()        

    def commit(self):
        """
        If a scoped_session exist makes a commit and return the result. If is not exist, commit the existing session.

        Returns:
            _type_: Commit return.
        """
        if self.scoped_session:
            return self.scoped_session.commit()
        elif self.session:
            return self.session.commit()

    def rollback(self):
        """
        If a scoped_session exist makes a rollback and return the result. If is not exist, rollback the existing session.

        Returns:
            _type_: Rollback return.
        """
        if self.scoped_session:
            return self.scoped_session.rollback()
        elif self.session:
            return self.session.rollback()
        
    def rollback_own(self):
        """
        Rollback the existing session, not the scoped session that must be closed by the caller

        Returns:
            _type_: Rollback return.
        """
        if self.session:
            return self.session.rollback()
        
    def get_session(self) -> Session:
        """
        Returns the existing scoped_session, if is not exist, return the existing session and if is not exist create a new session and return it.

        Returns:
            Session: DB session.
        """
        return self.getsession()
        
    def getsession(self) -> Session:
        """
        Returns the existing scoped_session, if is not exist, return the existing session and if is not exist create a new session and return it.

        Returns:
            Session: DB session.
        """
        if self.scoped_session:
            return self.scoped_session
        elif self.session:
            return self.session
        else:
            self.session = DBBase.gDBSession()
            return self.session
        

    def GetDBSessionLog(self) -> Session:
        """
        Creates a new session for DB log.

        Returns:
            Session: DB Session.
        """

        BaseLog.metadata.bind = DBBase.gDbEngineLog
        dbsessionLog = sessionmaker(bind=DBBase.gDbEngineLog, autoflush=True)
        session = dbsessionLog()
        return session
    
    @staticmethod
    def log_action(id_user: str, type: str="info", message: str="", info: dict={}, log2Use: logging.Logger = None,ip_origin=None,id_company=None):
        """Records a new event in the DB log table.

        Args:
            id_user (str): _description_
            type (str, optional): _description_. Defaults to "info".
            message (str, optional): _description_. Defaults to "".
            info (dict, optional): _description_. Defaults to {}.
            log2Use (logging.Logger, optional): _description_. Defaults to None.
            ip_origin (_type_, optional): _description_. Defaults to None.
            id_company (_type_, optional): _description_. Defaults to None.
        """        
        db=DBBase()
        db.Log(id_user=id_user,err_type=type,message=message,info=info,log2Use=log2Use,ip_origin=ip_origin,id_company=id_company)

    def Log(self, id_user: str, err_type: str, message: str, info: dict={}, log2Use: logging.Logger = None,ip_origin=None,id_company=None):
        """
        Records a new event in the DB log table.

        Args:
            id_user (str): User id.
            err_type (str): Error Type.
            message (str): Error message.
            info (dict): Process info.
            log2Use (logging.Logger, optional): Logger to use in console. Defaults to None.
            ip_origin (_type_, optional): _description_. Defaults to None.
            id_company (_type_, optional): _description_. Defaults to None.
        """
        if log2Use is None:
            log2Use = logging.getLogger(__name__)
        session = None
        try:
            if err_type == 'debug':
                return

            log2Use.info('%s, %s, %s, %s' %
                         (str(id_user), err_type, message, str(info)))
            self.insert_log_entry(id_user=id_user, log_type=err_type, message=message, info=info,ip_origin=ip_origin,id_company=id_company)
        except Exception as oEx:
            log2Use.error(str(oEx))
            # we can not raise this exceptions to user.only in dev mode
            # TODO: crear un flag de configuracion para enviar este error a cierto tipo de usuarios/grupos/ o en desarrollo
            # raise DBException('1.255.2', 'log', 'Exception adding log: %s' % str(oEx))
        finally:
            if session:
                session.close()
                

    def insert_log_entry(self, id_user: str, log_type: str, message: str, info: dict = None, ip_origin: str = None,id_company=None):
        query = """
            INSERT INTO log ("type", "date", message, info, id_user, ip_origin,id_company)
            VALUES (:log_type, DEFAULT, :message, :info, :id_user, :ip_origin, :id_company)            
        """
        query=self.parse_sql(query)
        # Ensure optional parameters have default values
        if info is None:
            info = {}
        
        # Prepare parameters for the query
        params = {
            "log_type": log_type,
            "message": message,
            "info": info,
            "id_user": id_user,
            "ip_origin": ip_origin,
            "id_company": id_company
        }

        # Use the existing SQLAlchemy engine connection
        with self.gDbEngineLog.connect() as connection:
            # Execute the query with parameters
            result = connection.execute(query, params)
            
            # Fetch and return the generated ID
            # inserted_id = result.fetchone()[0]
            # return inserted_id



    def list2dict(self, queryList: list) -> list:
        """
        Returns a list of objects as dict.

        Args:
            queryList (list): List of objects.

        Returns:
            list: List of objects as dict.
        """
        listItems = []
        for _row in queryList:
            listItems.append(Row2Dict.row2dict(_row))
        return listItems

    def row2dict(self, row) -> dict:
        """
        Converts a row object to a dict.

        Args:
            row (_type_): Object to convert to Dict. The type of object varies depending on the object.

        Returns:
            dict: Object dict.
        """
        return Row2Dict.row2dict(row)
    
    def getRowData(self, row) -> dict:
        """
        Converts a row object to a dict.

        Args:
            row (_type_): Object to convert to Dict. The type of object varies depending on the object.

        Returns:
            dict: Object dict.
        """
        return Row2Dict.getRowData(row)   
     
    def convertRowJsonToDict(self, asjsonb) -> dict:
        """
        Converts a row object to a dict.

        Args:
            row (_type_): Object to convert to Dict. The type of object varies depending on the object.

        Returns:
            dict: Object dict.
        """
        return Row2Dict.convertRowJsonToDict(asjsonb)       

    def getFilterSelect(self, filter: dict, sSELECT: str) -> str:
        """
        Returns the select statement. If the filter contains columns, they are added to the statement.

        Args:
            filter (dict): Filter with the columns.
            sSELECT (str): Select statement.

        Returns:
            str: Select statement result.
        """
        sSQL = ''
        if filter is None:
            sSQL = sSELECT
        elif 'columns' in filter:
            sSQL = 'SELECT '
            bFirst = True
            for c in filter['columns']:
                if not bFirst:
                    sSQL = sSQL + ','
                else:
                    bFirst = False
                sSQL = sSQL + c
        else:
            sSQL = sSELECT

        return sSQL

    def getFilter(self, filter: dict, sSELECT: str, sFROM: str, sWHERE: str) -> str:
        """
        Returns the select statement with filters.

        Args:
            filter (dict): Filters.
            sSELECT (str): Main select statement.
            sFROM (str): From statement.
            sWHERE (str): Where statement.

        Returns:
            str: Final select statement.
        """

        sORDERBY = ''
        sGROUPBY = ''
        sLIMIT = ''
        sOFFSET = ''
        sINNERJOIN = ''
        if filter is None:
            filter = {}

        sSQL = self.getFilterSelect(filter, sSELECT)
        sSQL = sSQL + sFROM

        if 'innerjoin' in filter:
            for inner in filter['innerjoin']:
                sINNERJOIN = sINNERJOIN + 'INNER JOIN ' + inner + ' '

        sSQL = sSQL + sINNERJOIN

        if 'filter' in filter:
            findWhere=sWHERE.find("WHERE")
            filter_where_predicate=filter['filter']
            if findWhere==-1:
                #solo lo añade si no encuentra ningun where, pero puede haber un where en subconsulta 
                # con lo que fallaria, por lo menos cubrimos uno de los casos
                sWHERE= " WHERE "
            if filter_where_predicate!='':
                sWHERE = sWHERE + ' AND '      
            sWHERE = sWHERE + filter_where_predicate

        sSQL = sSQL + sWHERE

        if 'orderby' in filter:
            sORDERBY = sORDERBY + ' ORDER BY %s ' % filter['orderby']
        if 'groupby' in filter:
            sGROUPBY = sGROUPBY + ' GROUP BY %s ' % filter['groupby']
        if 'limit' in filter:
            sLIMIT = sLIMIT + ' LIMIT %d ' % filter['limit']
        if 'offset' in filter:
            sOFFSET = sOFFSET + ' OFFSET %d ' % filter['offset']

        sSQL = sSQL + sGROUPBY + sORDERBY + sLIMIT + sOFFSET
        # self.log.debug(sSQL)
        return sSQL

    def transformGeom(self, geom: dict, coords: int = 4326) -> ST_SetSRID:
        """
        Tranform a json geometry to a geoalchemy2 Polygon.

        Args:
            geom (dict): json geom.
            coords (int, optional): EPSG coords. Defaults to 4326.

        Returns:
            ST_SetSRID: Polygon.
        """
        return func.st_setsrid(func.st_force2d(func.st_geomfromgeojson(json.dumps(geom))), coords)
    
    def transformGeom3d(self, geom: dict, coords: int = 4326) -> ST_SetSRID:
        """
        Tranform a json geometry to a geoalchemy2 Polygonz.

        Args:
            geom (dict): json geom.
            coords (int, optional): EPSG coords. Defaults to 4326.

        Returns:
            ST_SetSRID: Polygonz.
        """
        return func.st_setsrid(func.st_force3d(func.st_geomfromgeojson(json.dumps(geom))), coords)    

    def createPolygon4326(self, geom: dict) -> ST_SetSRID:
        """
        Tranform a json polygon to a geoalchemy2 Polygon.

        Args:
            geom (dict): json polygon. with the geom in 'rings' field and the EPSG in 'spatialReference' field.

        Returns:
            ST_SetSRID: Polygon.
        """
        data_set = {"type": "Polygon", "coordinates": geom['rings']}
        coords = geom['spatialReference']['wkid']
        return func.st_setsrid(func.st_force2d(func.st_geomfromgeojson(json.dumps(data_set))), coords)
    
    def transformGeometryToJson(self, geom: Any) -> ST_SetSRID:
        """convert a geometry to json as a GeoJSON dict. There is no check that the GeoJSON is using an SRID of 4326.

        Args:
            geom (Any): geometry value

        Returns:
            ST_SetSRID: dict
        """        
        return Row2Dict.transformGeometryToJson(geom=geom)

    @staticmethod
    def getServerDateTimeWithZone() -> datetime:
        """
        Returns the current server datetime in UTC.

        Returns:
            datetime.datetime: current server datetime.
        """
        return DateHelper.getServerDateTimeWithZone()
    @staticmethod
    def getServerDateTimeWithZoneString() -> str:
        """
        Returns the string with the current server datetime in UTC.

        Returns:
            str: current server datetime in string.
        """
        return str(DBBase.getServerDateTimeWithZone())

    def update_field(self, field: str, new_data: dict, item2Update):
        """update generic field without transformation

        Args:
            field (str): field name
            new_data (dict): dict that contain the entry
            target (_type_): target field object
        """
        if field in new_data:
            setattr(item2Update, field, new_data[field])

    def updateFieldsByList(self, item2Update, newItem: dict, fields: list = []):
        """update array list of fields between new data and target object

        Args:
            item2Update (_type_): _description_
            newItem (dict): _description_
            fields (list, optional): _description_. Defaults to [].
        """
        for column in fields:
            self.update_field(column, newItem, item2Update)

    def updateFields(self, item2Update, newItem: dict, excludeFields: list = []):
        """
        Updates generic fields of an object.

        Args:
            item2Update (_type_): item target to update, must be a model class
            newItem (dict): dictionary with the data, normally from client
            excludeFields (list, optional): list the fields to avoid in the update. Defaults to [].
        """

        for column in item2Update.__table__.columns:
            if column.name in excludeFields:
                continue
            _attr = getattr(item2Update, column.name)
            # _type=type(_attr)
            # _type2=column.type
            # print("column.name:" + column.name + " is " + str(type(_attr)) + "   " + str(column.type))
            if column.name in newItem:
                _newvalue = newItem[column.name]
                # todo: validate field type
                if isinstance(_attr, datetime):
                    # print("is datetime")
                    setattr(item2Update, column.name, _newvalue)
                elif isinstance(_attr, list):
                    # print("is list")
                    setattr(item2Update, column.name, _newvalue)
                elif isinstance(_attr, dict):
                    # print("is dict")
                    setattr(item2Update, column.name, _newvalue)
                else:
                    # print("is generic")
                    setattr(item2Update, column.name, _newvalue)

    def updateFieldJson(self, item2Update, newItem: dict, json_column: str = "info", excludeFields: list = []):
        """update json fields recursively

        Args:
            item2Update (any): object to update
            newItem (dict): dict with the data to update.
            json_column (str, optional): name of the json column. Defaults to "info".
            excludeFields (list, optional): list of fields to avoid to edit. Defaults to [].
        """
        infoChanged = False
        if newItem is None or item2Update is None:
            return

        new_info = newItem.get(json_column)
        if new_info is None:
            return

        json_data = getattr(item2Update, json_column)

        if json_data is None or json_data == '':
            json_data = {}
            infoChanged = True

        for k in new_info:
            if k in excludeFields:
                continue
            old_value = json_data.get(k)
            new_value = new_info.get(k)
            if old_value != new_value:
                json_data[k] = new_value
                infoChanged = True

        if infoChanged == True:
            DBBase.markJsonAsModified_st(item2Update=item2Update,jsonField= json_column)
    @staticmethod
    def markJsonAsModified_st(item2Update, jsonField: str="info"):
        if isinstance(item2Update, Dict):
            pass #only for sqlalchemy objects
        else:            
            flag_modified(item2Update, jsonField)           

    def markJsonAsModified(self, item2Update, jsonField: str="info"):
        DBBase.markJsonAsModified_st(item2Update=item2Update,jsonField= jsonField)

    @staticmethod
    def updateModifyTime_st(ruser: any, item2Update, updating_date=None):
        """
        Updates generic fields for updating.

        Args:
            ruser (dict|str): User data or id.
            item2Update (_type_): Object to be updated.
        """
        if item2Update is None:
            return
        info = None
        if isinstance(item2Update, Dict):
            info = item2Update["info"]
        else:
            info = item2Update.info

        if info is None or info == '':
            info = {}

        if isinstance(ruser, Dict):
            info['updated_by'] = str(ruser['id'])
        else:
            info['updated_by'] = str(ruser)
        
        # date
        str_date = DBBase.getServerDateTimeWithZoneString()
        if updating_date is not None:
            if isinstance(updating_date, datetime):
                str_date = DateHelper.datetime2StringWithZone(updating_date)
            else:
                str_date =  updating_date

        info['updating_date'] =  str_date
        DBBase.markJsonAsModified_st(item2Update=item2Update)

    def updateModifyTime(self, ruser: any, item2Update, updating_date=None):
        """
        Updates generic fields for updating.

        Args:
            ruser (dict|str): User data.
            item2Update (_type_): Object to be updated.
        """
        return DBBase.updateModifyTime_st(ruser=ruser,item2Update=item2Update,updating_date=updating_date)
    
    def insertCreationTime(self, ruser: any, item2Update, updating_date=None):
        """
        create generic fields for creation.

        Args:
            ruser (dict|str): User data. If dict it must contains de id else the id.
            item2Update (any|dict): Object to be updated.
        """
        if item2Update is None:
            return
        info = None
        if isinstance(item2Update, Dict):
            info = item2Update["info"]
        else:
            info = item2Update.info

        if info is None or info == '':
            info = {}


        if isinstance(ruser, Dict):
            info['created_by'] = str(ruser['id'])
        else:
            info['created_by'] = str(ruser)
        # date
        str_date = self.getServerDateTimeWithZoneString()
        if updating_date is not None:
            if isinstance(updating_date, datetime):
                str_date = DateHelper.datetime2StringWithZone(updating_date)
            else:
                str_date =  updating_date

        info['creation_date'] =  str_date
        self.updateModifyTime(ruser=ruser,item2Update=item2Update,updating_date=str_date)

    def get_logger(self):
        # Extraer el nombre de la clase que está llamando
        frame = inspect.currentframe().f_back
        class_name = frame.f_locals.get('self', None).__class__.__name__
        
        # Crear un logger específico para esa clase
        logger = logging.getLogger(class_name)
        return logger   
    
    def handleException(self
                        ,oEx
                        ,code="0.0.0.1"
                        ,operation="Generic Error"
                        ,message:str='Error: %s'
                        ,logException:bool=True) -> DBException:
        """
        Handle a Generic Exception Response.

        Args:
            oEx (Exception): Exception
            code (str, optional): Code of the error. Defaults to "0.0.0.1".
            operation (str, optional): Description fo the operation. Defaults to "Generic Error".
            message (_type_, optional): Message to include the base message. Defaults to 'Error: %s'.
            logException (bool, optional): True to log the error. Defaults to True.

        Returns:
            DBException: DBException
        """        
        if (logException):
            logger =self.get_logger()
            logger.error(oEx)
                  
        if (type(oEx) is  DBException ):
            return oEx
        else: 
            nex=DBException(code=code, operation= operation,message= message,exception=oEx) 
        return nex


    def saveToDisk(self, req, outputFilePath):
        
        self.log.info('Writing to file: %s ...' % outputFilePath)
        if not os.path.exists(os.path.split(outputFilePath)[0]):
            os.makedirs(os.path.split(outputFilePath)[0])
            os.chmod(os.path.split(outputFilePath)[0], 0o777)
            if not os.path.exists(os.path.split(outputFilePath)[0]):
                self.log.error('Something is wrong with the "id". The server directory doesn\'t exist')
                raise Exception('Something is wrong with the "id". The server directory doesn\'t exist')
        
        info = json.loads(req.get_param('info'))
        file = req.get_param('file')
        
        tInit = dt.datetime.now()
        try:
            self.log.info('Creating file: %s' % outputFilePath)
            fout = open(outputFilePath, 'wb')
        except Exception as oEx:
            self.log.error(oEx)
            raise
        if not fout:
            self.log.error('Opening file for write: %s' % (outputFilePath))
            raise Exception(f"Could not  write file {outputFilePath}")

        if 'filesize' not in info or info['filesize'] is None:
            filesize = req.content_length
        else:
            filesize = info['filesize']
        if 'md5' not in info or info['md5'] is None:
            md5Checksum = None
        else:
            md5Checksum = info['md5']
        md5 = hashlib.md5()
        chunksize = 1024*1024*64
        iTotalReaded = 0
        if filesize is None or filesize < chunksize:
            data = file.file.read()
            md5.update(data)
            fout.write(data)
            iTotalReaded = iTotalReaded + len(data)
        else:
            nChunks = (int)(filesize / chunksize)
            if filesize % chunksize:
                nChunks = nChunks + 1
            #self.log.info('Reading %d chunks ...' % nChunks)
            for i in range(nChunks):
                #self.log.info('Reading chunks ... %d' % (i+1))
                data = file.file.read(chunksize)
                if not data:
                    break
                md5.update(data)
                iTotalReaded = iTotalReaded + len(data)
                fout.write(data)
        fout.close()
        md5calculated = md5.hexdigest()
        tEnd = dt.datetime.now()
        
        if md5calculated != md5Checksum:
            self.log.error('Checksum Failed: received %s, calculated [%s]' %(md5Checksum, md5calculated))
        self.log.info('Readed/Writed %s - %d bytes - %d seconds' % (file.filename, iTotalReaded, (tEnd-tInit).total_seconds()))
        return [md5calculated, iTotalReaded]
    

    def saveToRepository(self,app_config ,filename:str,inFilePath:str,repo_path:str):
        """generic method to save to repository

        Args:
            filename (str): file name to save Example: "file_123.jpg"
            inFilePath (str): input file path example "/dir1/file_XXXX.jpg"
            repo_path (str): repo path, we are using S3 and the repo configured in config file  so we need to use the last part "\dir\subir\"
        """        
        try:
            fileTool=FileTool(config_section_name= "REPOSITORY_CONFIG", global_config= app_config,log= self.log).create_FileManager()
            info_response = fileTool.saveFile(inFilePath,repo_path,filename)  
            return info_response      
        except Exception as oEx:
            raise

    def saveStreamToRepository(self,app_config ,stream, filename:str,repo_path:str):
        """generic method to save stream to repository

        Args:
            app_config (dict): app configuration for repository access
            stream (stream): stream to save
            filename (str): name to save
            repo_path (str): remote path where to save

        Returns:
            (dict): dict with response from remote operation
        """
              
        """

        Args:
            repo_path (str): repo path, we are using S3 and the repo configured in config file  so we need to use the last part "\dir\subir\"
        """  
        try:
            fileTool=FileTool(config_section_name= "REPOSITORY_CONFIG", global_config= app_config,log= self.log).create_FileManager()
            info_response = fileTool.saveObject(stream,repo_path,filename)  
            return info_response      
        except Exception as oEx:
            raise
        
    @staticmethod
    def parse_sql(sql):     
        from sqlalchemy import text as sqltext   
        return sqltext(sql)
        
    def compare_update_json(self,item2Modify:dict,target_info:dict,changed_data={},avoid_fields=None):
        """Compare data in json to see if there are changes. If avoid_fields is None then ["updating_date","updated_by","created_by","creation_date"] will be used by default

        Args:
            item2Modify (dict): ned data
            target_info (dict): json data to be edited
            changed_data (dict, optional): changed data by reference. Defaults to {}.
            avoid_fields (list, optional): fields to avoid in the comparison. Defaults to None but the will be use updating d.

        Returns:
            _type_: _description_
        """     
        has_changes=False
        if avoid_fields is None:
            avoid_fields=["updating_date","updated_by","created_by","creation_date"]
        if 'info' in item2Modify:
            for k, v in item2Modify['info'].items():
                if k in avoid_fields:
                    continue
                if target_info.get(k) != v:
                    target_info[k] = v
                    has_changes = True
                    changed_data[k]=str(v)
        return {"has_changes":has_changes,"changed_data":changed_data}
    
    