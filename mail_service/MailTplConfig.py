'''
@author: Francisco R. Moreno Santana franrms@gmail.com
@copyright: (c) 2021 Francisco R. Moreno Santana franrms@gmail.com
@contact: francisco.moreno@geoaitech.com
'''
from sqlalchemy.dialects.postgresql import UUID, TEXT, TIMESTAMP, JSONB, BOOLEAN
from sqlalchemy import Column,  func, String
from ..modelhelper.Base import Base


class MailTplConfig(Base):
    __tablename__ = 'mail_tpl'
    
    id = Column(TEXT(), primary_key=True, nullable=False)
    description = Column(TEXT(), nullable=True)
    config = Column(JSONB(), nullable=True)
    tpl_data = Column(TEXT(), nullable=True)
    type = Column(String(256), nullable=True)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())  # Default to CURRENT_TIMESTAMP
    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())  # Auto-update on modification