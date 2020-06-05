from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Text, Float


Base = declarative_base()


class BaseSchema(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)


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
