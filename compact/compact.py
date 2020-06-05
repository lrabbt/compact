from sqlalchemy.orm import Session
from contextlib import contextmanager

import datetime
import csv

from .db import Opening, Closing, FileParsing
from .db import OK, FILE_NOT_FOUND, BROKEN_FILE
from .db import OPENING, CLOSING


class CompactConfiguration:
    def __init__(
            self,
            opening_file_path=None,
            closing_file_path=None,
            datasource_engine=None,
            output_file_path=None):
        self.opening_file_path = opening_file_path
        self.closing_file_path = closing_file_path
        self.datasource_engine = datasource_engine
        self.output_file_path = output_file_path


class Compact:
    def __init__(self, configuration: CompactConfiguration = None):
        self.configuration = configuration

    def compact(self):
        opening_entries = []
        closing_entries = []
        with self.db_session() as session:
            opening_action = FileParsing()
            opening_action.file_type = OPENING
            opening_action.file_path = self.configuration.opening_file_path
            opening_action.status = OK
            try:
                with open(self.configuration.opening_file_path,
                          'r', newline='') as opening_file:
                    opening_reader = csv.DictReader(opening_file,
                                                    delimiter=';')
                    for row in opening_reader:
                        day, month, year = map(
                            int, row['DATA_INICIO'].strip().split('/'))
                        initial_date = datetime.datetime(year, month, day)
                        opening = Opening(id=row['ID'],
                                          initial_date=initial_date,
                                          name=row['NOME'],
                                          note=row['NOTA'],
                                          unit=row['UNIDADE'])

                        opening_entries.append(row)
                        session.add(opening)
            except FileNotFoundError:
                opening_action.status = FILE_NOT_FOUND
            except KeyError:
                opening_action.status = BROKEN_FILE

            session.add(opening_action)

            closing_action = FileParsing()
            closing_action.file_type = CLOSING
            closing_action.file_path = self.configuration.closing_file_path
            closing_action.status = OK
            try:
                with open(self.configuration.closing_file_path,
                          'r', newline='') as closing_file:
                    closing_reader = csv.DictReader(closing_file,
                                                    delimiter=';')
                    for row in closing_reader:
                        date, time = row['DATA_FIM'].strip().split()
                        day, month, year = map(int, date.split('/'))
                        hour, minute = map(int, time.split(':'))
                        final_date = datetime.datetime(
                            year, month, day, hour, minute)
                        value = float(row['VALOR'].replace(',', '.'))
                        closing = Closing(id=row['ID'],
                                          final_date=final_date,
                                          value=value)

                        closing_entries.append(row)
                        session.add(closing)
            except FileNotFoundError:
                closing_action.status = FILE_NOT_FOUND
            except KeyError:
                closing_action.status = BROKEN_FILE

            session.add(closing_action)

        with open(self.configuration.output_file_path, 'w') as f:
            fieldnames = [
                'ID',
                'DATA_INICIO',
                'NOME',
                'NOTA',
                'UNIDADE',
                'DATA_FIM',
                'VALOR']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for opening in opening_entries:
                for closing in closing_entries:
                    if opening['ID'] == closing['ID']:
                        full_cycle = {**opening, **closing}
                        writer.writerow(full_cycle)

    @contextmanager
    def db_session(self):
        session = Session(bind=self.configuration.datasource_engine)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close
