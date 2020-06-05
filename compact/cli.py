from sqlalchemy import create_engine
from pathlib import Path

from .compact import CompactConfiguration
from .compact import Compact
from .db import Base

import click


DEFAULT_DATA_PATH = Path.home() / '.local' / 'share' / 'compact'
DEFAULT_DATASOURCE_URL = f'sqlite:///{DEFAULT_DATA_PATH / "compact.db"}'


@click.command()
@click.option('-io', '--input-opening', type=click.Path(),
              default=DEFAULT_DATA_PATH / 'openings.csv',
              help='First (opening) data file')
@click.option('-ic', '--input-closing', type=click.Path(),
              default=DEFAULT_DATA_PATH / 'closings.csv',
              help='Second (closing) data file')
@click.option('-d', '--datasource',
              default=DEFAULT_DATASOURCE_URL,
              help='Datasource URL')
@click.option('-o', '--output-file', type=click.Path(),
              default=DEFAULT_DATA_PATH / 'compacted.csv',
              help='Output file')
@click.option('-a', '--actions-file', type=click.Path(),
              default=DEFAULT_DATA_PATH / 'actions.csv',
              help='Actions (log) file')
def main(input_opening, input_closing, datasource,
         output_file, actions_file):
    DEFAULT_DATA_PATH.mkdir(parents=True, exist_ok=True)

    config = CompactConfiguration()
    config.opening_file_path = Path(input_opening)
    config.closing_file_path = Path(input_closing)
    config.datasource_engine = create_engine(datasource)
    config.output_file_path = Path(output_file)
    config.actions_file_path = Path(actions_file)

    compact = Compact(config)
    compact.compact()


@click.group()
def utils():
    pass


@utils.command()
@click.option('-d', '--datasource',
              default=DEFAULT_DATASOURCE_URL,
              help='Datasource URL')
def init(datasource):
    DEFAULT_DATA_PATH.mkdir(parents=True, exist_ok=True)
    engine = create_engine(datasource)
    Base.metadata.create_all(bind=engine)


@utils.command()
@click.option('-d', '--datasource',
              default=DEFAULT_DATASOURCE_URL,
              help='Datasource URL')
def db_reset(datasource):
    DEFAULT_DATA_PATH.mkdir(parents=True, exist_ok=True)
    engine = create_engine(datasource)
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
