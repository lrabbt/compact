from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import func
from sqlalchemy import Column, Integer, String, DateTime, Text, Float

from pathlib import Path


OK = 'OK'
FILE_NOT_FOUND = 'Arquivo inexistente'
BROKEN_FILE = 'Arquivo corrompido'
REPEATED_ID = 'Id repetido'

OPENING = 'OPENING'
CLOSING = 'CLOSING'

Base = declarative_base()


class BaseSchema(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    creation_date = Column(DateTime(timezone=True), server_default=func.now())


class Opening(BaseSchema):
    __tablename__ = 'TB_DADOS01'

    initial_date = Column('data_inicio', DateTime)
    name = Column('nome', String(255))
    note = Column('nota', Text)
    unit = Column('unidade', Integer)

    def __init__(
            self,
            id=None,
            initial_date=None,
            name=None,
            note=None,
            unit=None):
        self.id = id
        self.initial_date = initial_date
        self.name = name
        self.note = note
        self.unit = unit


class Closing(BaseSchema):
    __tablename__ = 'TB_DADOS02'

    final_date = Column('data_fim', DateTime)
    value = Column('valor', Float)


class FileParsing(BaseSchema):
    __tablename__ = 'TB_LOG'

    file_type = Column('tipo_arquivo', String(255))
    _file_path = Column('caminho_arquivo', String(255))
    status = Column('status', String(255))

    @hybrid_property
    def file_path(self):
        return Path(self._file_path)

    @file_path.setter
    def file_path(self, file_path):
        self._file_path = str(file_path)
