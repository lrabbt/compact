from compact.compact import Compact, CompactConfiguration
from compact.db import Base, Opening, Closing, FileParsing
from compact.db import OPENING, CLOSING, OK, FILE_NOT_FOUND, BROKEN_FILE
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

import pytest
import csv


@pytest.fixture
def engine(tmp_path):
    db_path = tmp_path / 'test.db'
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(bind=engine)
    return engine


@pytest.fixture
def db_session(engine):
    session = Session(bind=engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@pytest.fixture
def sample_data_opening():
    return dict(
        fieldnames=['ID', 'DATA_INICIO', 'NOME', 'NOTA', 'UNIDADE'],
        entries=[
            dict(id=1, initial_date='12/12/2019',
                 name='Jose Luiz Bordonal Junior',
                 note=' CADQOS-PRD', unit=11),
            dict(id=2, initial_date='13/12/2019',
                 name='Jaqueline Aparecida Ferreira',
                 note=' CATI_PRD-Agenda', unit=387),
            dict(id=3, initial_date='14/12/2019',
                 name='Jaqueline Aparecida Ferreira',
                 note=' CATI_PRD-Entr.Saida', unit=420),
            dict(id=4, initial_date='15/12/2019',
                 name='Jaqueline Aparecida Ferreira',
                 note=' CATI_PRD-Eventual', unit=5),
            dict(id=5, initial_date='16/12/2019',
                 name='Jaqueline Aparecida Ferreira',
                 note=' CATI_PRD-Relatorios', unit=85),
            dict(id=6, initial_date='17/12/2019',
                 name='Jaqueline Aparecida Ferreira',
                 note=' CATI_PRD-Sched', unit=17),
            dict(id=7, initial_date='18/12/2019',
                 name='Jaqueline Aparecida Ferreira',
                 note=' CATI_Producao', unit=1),
            dict(id=8, initial_date='19/12/2019',
                 name='Jose Luiz Bordonal Junior',
                 note=' Clarify_Producao', unit=91),
            dict(id=9, initial_date='20/12/2019',
                 name='Norishigue Claudio Miyashiro',
                 note=' FMS-PRD', unit=67),
            dict(id=10, initial_date='21/12/2019',
                 name='Jose Luiz Bordonal Junior',
                 note=' IFT_Producao', unit=2),
            dict(id=11, initial_date='22/12/2019',
                 name='Jose Luiz Bordonal Junior',
                 note=' Integra_Producao', unit=53),
            dict(id=12, initial_date='23/12/2019',
                 name='Karlo Augusto Pedro Franco Correa',
                 note=' Intermediate', unit=27),
            dict(id=13, initial_date='24/12/2019',
                 name='Fabio Amauri Puerta',
                 note=' MasterDB_Avulsos', unit=6),
            dict(id=14, initial_date='25/12/2019',
                 name='Fabio Amauri Puerta',
                 note=' MasterDB_Carga', unit=7),
            dict(id=15, initial_date='26/12/2019',
                 name='Fabio Amauri Puerta',
                 note=' MasterDB_FTP_Gateway', unit=16),
            dict(id=16, initial_date='27/12/2019',
                 name='Fabio Amauri Puerta',
                 note=' MasterDB_Limpeza', unit=7),
            dict(id=17, initial_date='28/12/2019',
                 name='Karlo Augusto Pedro Franco Correa',
                 note=' Mediation_Producao', unit=143),
            dict(id=18, initial_date='29/12/2019',
                 name='Jose Luiz Bordonal Junior',
                 note=' PROD_Query_CLARIFY', unit=11),
            dict(id=19, initial_date='30/12/2019',
                 name='Norishigue Claudio Miyashiro',
                 note=' PUF-CCIN-Prod', unit=606),
            dict(id=20, initial_date='31/12/2019',
                 name='Norishigue Claudio Miyashiro',
                 note=' PUF-GPF-Prod', unit=124),
            dict(id=21, initial_date='01/01/2020',
                 name='Fabio Amauri Puerta',
                 note=' Portabil_Scheduler', unit=32),
            dict(id=22, initial_date='02/01/2020',
                 name='Fabio Amauri Puerta',
                 note=' Portabilidade', unit=145),
            dict(id=23, initial_date='03/01/2020',
                 name='Fabio Amauri Puerta',
                 note=' Preparo_Controle', unit=9),
            dict(id=24, initial_date='04/01/2020',
                 name='Elton Totoli',
                 note=' RJO_PRD_ADM_CTM_ITLG', unit=6),
            dict(id=25, initial_date='05/01/2020',
                 name='Karlo Augusto Pedro Franco Correa',
                 note=' RJO_PRD_ADM_ESDP_MNT', unit=33),
            dict(id=26, initial_date='06/01/2020',
                 name='Karlo Augusto Pedro Franco Correa',
                 note=' RJO_PRD_ADM_OLM_MNTD', unit=2),
            dict(id=27, initial_date='07/01/2020',
                 name='Norishigue Claudio Miyashiro',
                 note=' RJO_PRD_ATF_SNE_FMS', unit=105),
            dict(id=28, initial_date='08/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_COM_ACC_RPCC', unit=1),
            dict(id=29, initial_date='09/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_COM_DST_REM', unit=177),
            dict(id=30, initial_date='10/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_COM_MENSAL', unit=176),
            dict(id=31, initial_date='11/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_COM_OIC_D', unit=70),
            dict(id=32, initial_date='12/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_COM_REM_D', unit=55),
            dict(id=33, initial_date='13/01/2020',
                 name='Fabio Amauri Puerta',
                 note=' RJO_PRD_COM_UCM', unit=92),
            dict(id=34, initial_date='14/01/2020',
                 name='Andre Luiz Lima',
                 note=' RJO_PRD_DLP_TRANSF', unit=1),
            dict(id=35, initial_date='15/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_ADHOC', unit=10),
            dict(id=36, initial_date='16/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BASE_DESV', unit=7),
            dict(id=37, initial_date='17/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BASE_OFERTA', unit=10),
            dict(id=38, initial_date='18/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BDO', unit=101),
            dict(id=39, initial_date='19/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BIGDATA_CUSTOMER_EXPERIENCE', unit=40),
            dict(id=40, initial_date='20/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BIGDATA_FATURAMENTO', unit=9),
            dict(id=41, initial_date='21/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BIGDATA_METRICAS', unit=1),
            dict(id=42, initial_date='22/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BIGDATA_PESQUISA_REDE', unit=1),
            dict(id=43, initial_date='23/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BIGDATA_PESQUISA_SATISFACAO', unit=1),
            dict(id=44, initial_date='24/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BIGDATA_RECARGAS_ATIVADAS', unit=6),
            dict(id=45, initial_date='25/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BIGDATA_REPORTS_RTDM', unit=1),
            dict(id=46, initial_date='26/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BIGDATA_SAS_GPRS', unit=27),
            dict(id=47, initial_date='27/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BIGDATA_SAS_ID', unit=60),
            dict(id=48, initial_date='28/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BLT', unit=1),
            dict(id=49, initial_date='29/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_BSCS', unit=4),
            dict(id=50, initial_date='30/01/2020',
                 name='Andressa Ferrazza Gentil',
                 note=' RJO_PRD_DWH_CAC', unit=330)])


@pytest.fixture
def sample_data_closing():
    return dict(
        fieldnames=['ID', 'DATA_FIM', 'VALOR'],
        entries=[
            dict(id=1, final_date='12/12/2020 10:01', value='11,00'),
            dict(id=2, final_date='13/12/2020 11:01', value='387,00'),
            dict(id=3, final_date='14/12/2020 15:01', value='420,00'),
            dict(id=4, final_date='15/12/2020 10:01', value='5,00'),
            dict(id=5, final_date='16/12/2020 10:01', value='85,00'),
            dict(id=6, final_date='17/12/2020 10:01', value='17,00'),
            dict(id=7, final_date='18/12/2020 23:01', value='1,00'),
            dict(id=8, final_date='19/12/2020 10:01', value='91,00'),
            dict(id=9, final_date='20/12/2020 10:01', value='67,00'),
            dict(id=10, final_date='21/12/2020 10:01', value='2,00'),
            dict(id=11, final_date='22/12/2020 10:01', value='53,00'),
            dict(id=12, final_date='23/12/2020 10:01', value='27,55'),
            dict(id=13, final_date='24/12/2020 10:01', value='6,00'),
            dict(id=14, final_date='25/12/2020 10:01', value='7,00'),
            dict(id=15, final_date='26/12/2020 10:01', value='16,90'),
            dict(id=16, final_date='27/12/2020 10:01', value='7,00'),
            dict(id=17, final_date='28/12/2020 10:01', value='143,00'),
            dict(id=18, final_date='29/12/2020 10:01', value='11,00'),
            dict(id=19, final_date='30/12/2020 10:01', value='606,30'),
            dict(id=20, final_date='31/12/2020 10:01', value='124,00'),
            dict(id=21, final_date='01/01/2021 10:01', value='32,00'),
            dict(id=22, final_date='02/01/2021 10:01', value='145,00'),
            dict(id=23, final_date='03/01/2021 10:01', value='9,00'),
            dict(id=24, final_date='04/01/2021 10:01', value='6,00'),
            dict(id=25, final_date='05/01/2021 10:01', value='33,00'),
            dict(id=26, final_date='06/01/2021 10:01', value='2,00'),
            dict(id=27, final_date='07/01/2021 10:01', value='105,00'),
            dict(id=28, final_date='08/01/2021 10:01', value='1,00'),
            dict(id=29, final_date='09/01/2021 10:01', value='177,00'),
            dict(id=30, final_date='10/01/2021 10:01', value='176,00'),
            dict(id=31, final_date='11/01/2021 10:01', value='70,00'),
            dict(id=32, final_date='12/01/2021 10:01', value='55,00'),
            dict(id=33, final_date='13/01/2021 10:01', value='92,00'),
            dict(id=34, final_date='14/01/2021 10:01', value='1,00'),
            dict(id=35, final_date='15/01/2021 10:01', value='10,00'),
            dict(id=36, final_date='16/01/2021 10:01', value='7,00'),
            dict(id=37, final_date='17/01/2021 10:01', value='10,11'),
            dict(id=38, final_date='18/01/2021 10:01', value='101,00'),
            dict(id=39, final_date='19/01/2021 10:01', value='40,00'),
            dict(id=40, final_date='20/01/2021 10:01', value='9,00'),
            dict(id=41, final_date='21/01/2021 10:01', value='1,00'),
            dict(id=42, final_date='22/01/2021 10:01', value='1,00'),
            dict(id=43, final_date='23/01/2021 10:01', value='1,10'),
            dict(id=44, final_date='24/01/2021 10:01', value='6,00'),
            dict(id=45, final_date='25/01/2021 10:01', value='1,00'),
            dict(id=46, final_date='26/01/2021 10:01', value='27,00'),
            dict(id=47, final_date='27/01/2021 10:01', value='60,00'),
            dict(id=48, final_date='28/01/2021 10:01', value='1,00'),
            dict(id=49, final_date='29/01/2021 10:01', value='4,00'),
            dict(id=50, final_date='30/01/2021 10:01', value='330,00')])


@pytest.fixture
def input_file_opening(tmp_path, sample_data_opening):
    opening_path = tmp_path / 'opening.csv'
    with open(opening_path, 'w') as f:
        writer = csv.DictWriter(f,
                                fieldnames=sample_data_opening['fieldnames'],
                                delimiter=';')

        writer.writeheader()
        for entry in sample_data_opening['entries']:
            row = dict(ID=entry['id'],
                       DATA_INICIO=entry['initial_date'],
                       NOME=entry['name'],
                       NOTA=entry['note'],
                       UNIDADE=entry['unit'])
            writer.writerow(row)

    return dict(path=opening_path,
                entries=sample_data_opening['entries'])


@pytest.fixture
def input_file_closing(tmp_path, sample_data_closing):
    fieldnames = sample_data_closing['fieldnames']
    entries = sample_data_closing['entries']
    closing_path = tmp_path / 'closing.csv'
    with open(closing_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')

        writer.writeheader()
        for entry in entries:
            row = dict(ID=entry['id'],
                       DATA_FIM=entry['final_date'],
                       VALOR=entry['value'])
            writer.writerow(row)

    return dict(path=closing_path,
                entries=entries)


@pytest.fixture
def input_files(tmp_path, input_file_opening, input_file_closing):
    return dict(opening=input_file_opening,
                closing=input_file_closing)


@pytest.fixture
def output_file_path(tmp_path):
    return tmp_path / 'dados3.csv'


@pytest.fixture
def actions_file_path(tmp_path):
    return tmp_path / 'out.log'


@pytest.fixture
def base_compact_configuration(input_files, engine,
                               output_file_path,
                               actions_file_path):
    return CompactConfiguration(
        opening_file_path=input_files['opening']['path'],
        closing_file_path=input_files['closing']['path'],
        datasource_engine=engine,
        output_file_path=output_file_path,
        actions_file_path=actions_file_path)


@pytest.fixture
def base_compact(base_compact_configuration):
    return Compact(base_compact_configuration)


def test_compact_database_save(base_compact, input_files, db_session):
    # when
    base_compact.compact()

    # then
    opening_entries = input_files['opening']['entries']
    closing_entries = input_files['closing']['entries']
    for opening in opening_entries:
        db_entry = db_session.query(Opening).filter_by(
            id=opening['id']).one_or_none()
        assert db_entry is not None, 'Entry not saved on database'
        assert db_entry.initial_date is not None, \
            'Initial date not saved on database'
        assert db_entry.name == opening['name'], 'Wrong name on database'
        assert db_entry.note == opening['note'], 'Wrong note on database'
        assert db_entry.unit == opening['unit'], 'Wrong unit on database'
    for closing in closing_entries:
        db_entry = db_session.query(Closing).filter_by(
            id=closing['id']).one_or_none()
        assert db_entry is not None, 'Entry not saved on database'
        assert db_entry.final_date is not None, \
            'Final date not saved on database'
        value = closing['value'].replace(',', '.')
        assert db_entry.value == float(value), 'Wrong value on database'


def test_compact_csv_generation(base_compact, input_files, output_file_path):
    # when
    base_compact.compact()

    # then
    opening_entries = input_files['opening']['entries']
    closing_entries = input_files['closing']['entries']
    for opening, closing in zip(opening_entries, closing_entries):
        output_row = None
        with open(output_file_path, 'r', newline='') as f:
            output_reader = csv.DictReader(f, delimiter=';')
            for row in output_reader:
                if int(opening['id']) == int(row['ID']):
                    output_row = row
                    break
        assert output_row is not None, 'Original data not saved on csv'
        assert output_row['DATA_INICIO'] == opening['initial_date'], \
            'Initial date wrong on csv'
        assert output_row['NOME'] == opening['name'], 'Name wrong on csv'
        assert output_row['NOTA'] == opening['note'], 'Note wrong on csv'
        assert output_row['UNIDADE'] == str(
            opening['unit']), 'Unit wrong on csv'
        assert output_row['DATA_FIM'] == closing['final_date'], \
            'Final date wrong on csv'
        assert output_row['VALOR'] == closing['value'], 'Value wrong on csv'


def test_compact_table_log(base_compact, input_files, db_session):
    # when
    base_compact.compact()

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == input_files['opening']['path'], \
        'Wrong opening file path on database'
    assert opening_action.status == OK, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == input_files['closing']['path'], \
        'Wrong closing file path on database'
    assert closing_action.status == OK, \
        'Wrong status saved on database'


def test_compact_file_log(base_compact, input_files, actions_file_path):
    # when
    base_compact.compact()

    # then
    assert actions_file_path.exists(), 'Log file not created'
    assert actions_file_path.is_file(), 'Log file is not a file'
    with actions_file_path.open() as f:
        lines = [line for line in f]
        assert len(lines) == 2, 'Log file badly saved'


def test_compact_opening_file_does_not_exist(tmp_path,
                                             base_compact_configuration,
                                             db_session, input_files):
    # given
    no_file_path = tmp_path / 'no-file.csv'
    base_compact_configuration.opening_file_path = no_file_path
    compact = Compact(base_compact_configuration)

    # when
    compact.compact()

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == no_file_path, \
        'Wrong opening file path on database'
    assert opening_action.status == FILE_NOT_FOUND, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == input_files['closing']['path'], \
        'Wrong closing file path on database'
    assert closing_action.status == OK, \
        'Wrong status saved on database'


def test_compact_closing_file_does_not_exist(tmp_path,
                                             base_compact_configuration,
                                             db_session, input_files):
    # given
    no_file_path = tmp_path / 'no-file.csv'
    base_compact_configuration.closing_file_path = no_file_path
    compact = Compact(base_compact_configuration)

    # when
    compact.compact()

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == input_files['opening']['path'], \
        'Wrong opening file path on database'
    assert opening_action.status == OK, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == no_file_path, \
        'Wrong closing file path on database'
    assert closing_action.status == FILE_NOT_FOUND, \
        'Wrong status saved on database'


def test_compact_opening_file_missing_key(tmp_path, sample_data_opening,
                                          base_compact_configuration,
                                          input_files, db_session):
    # given
    opening_file_path = tmp_path / 'no-key.csv'
    with open(opening_file_path, 'w') as f:
        fieldnames = sample_data_opening['fieldnames']
        fieldnames.pop(2)
        entries = sample_data_opening['entries']
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            delimiter=';')

        writer.writeheader()
        for entry in entries:
            row = dict(ID=entry['id'],
                       DATA_INICIO=entry['initial_date'],
                       NOTA=entry['note'],
                       UNIDADE=entry['unit'])
            writer.writerow(row)

    base_compact_configuration.opening_file_path = opening_file_path
    compact = Compact(base_compact_configuration)

    # when
    try:
        compact.compact()
    except Exception as e:
        assert e is None, 'No exceptions should be thrown'

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == opening_file_path, \
        'Wrong opening file path on database'
    assert opening_action.status == BROKEN_FILE, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == input_files['closing']['path'], \
        'Wrong closing file path on database'
    assert closing_action.status == OK, \
        'Wrong status saved on database'


def test_compact_closing_file_missing_key(tmp_path, sample_data_closing,
                                          base_compact_configuration,
                                          input_files, db_session):
    # given
    closing_file_path = tmp_path / 'no-key.csv'
    with open(closing_file_path, 'w') as f:
        fieldnames = sample_data_closing['fieldnames']
        fieldnames.pop(1)
        entries = sample_data_closing['entries']
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            delimiter=';')

        writer.writeheader()
        for entry in entries:
            row = dict(ID=entry['id'],
                       VALOR=entry['value'])
            writer.writerow(row)

    base_compact_configuration.closing_file_path = closing_file_path
    compact = Compact(base_compact_configuration)

    # when
    try:
        compact.compact()
    except Exception as e:
        assert e is None, 'No exceptions should be thrown'

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == input_files['opening']['path'], \
        'Wrong opening file path on database'
    assert opening_action.status == OK, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == closing_file_path, \
        'Wrong closing file path on database'
    assert closing_action.status == BROKEN_FILE, \
        'Wrong status saved on database'


def test_compact_opening_invalid_id(tmp_path, sample_data_opening,
                                    base_compact_configuration,
                                    db_session, input_files):
    # given
    opening_file_path = tmp_path / 'no-id.csv'
    with open(opening_file_path, 'w') as f:
        fieldnames = sample_data_opening['fieldnames']
        entries = sample_data_opening['entries']
        entries[2]['id'] = ''
        writer = csv.DictWriter(f,
                                fieldnames=fieldnames,
                                delimiter=';')

        writer.writeheader()
        for entry in entries:
            row = dict(ID=entry['id'],
                       DATA_INICIO=entry['initial_date'],
                       NOME=entry['name'],
                       NOTA=entry['note'],
                       UNIDADE=entry['unit'])
            writer.writerow(row)

    base_compact_configuration.opening_file_path = opening_file_path
    compact = Compact(base_compact_configuration)

    # when
    try:
        compact.compact()
    except Exception as e:
        assert e is None, 'No exceptions should be thrown'

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == opening_file_path, \
        'Wrong opening file path on database'
    assert opening_action.status == BROKEN_FILE, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == input_files['closing']['path'], \
        'Wrong closing file path on database'
    assert closing_action.status == OK, \
        'Wrong status saved on database'


def test_compact_opening_invalid_initial_date(tmp_path, sample_data_opening,
                                              base_compact_configuration,
                                              db_session, input_files):
    # given
    opening_file_path = tmp_path / 'no-id.csv'
    with open(opening_file_path, 'w') as f:
        fieldnames = sample_data_opening['fieldnames']
        entries = sample_data_opening['entries']
        entries[3]['initial_date'] = ''
        writer = csv.DictWriter(f,
                                fieldnames=fieldnames,
                                delimiter=';')

        writer.writeheader()
        for entry in entries:
            row = dict(ID=entry['id'],
                       DATA_INICIO=entry['initial_date'],
                       NOME=entry['name'],
                       NOTA=entry['note'],
                       UNIDADE=entry['unit'])
            writer.writerow(row)

    base_compact_configuration.opening_file_path = opening_file_path
    compact = Compact(base_compact_configuration)

    # when
    try:
        compact.compact()
    except Exception as e:
        assert e is None, 'No exceptions should be thrown'

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == opening_file_path, \
        'Wrong opening file path on database'
    assert opening_action.status == BROKEN_FILE, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == input_files['closing']['path'], \
        'Wrong closing file path on database'
    assert closing_action.status == OK, \
        'Wrong status saved on database'


def test_compact_opening_invalid_unit(tmp_path, sample_data_opening,
                                      base_compact_configuration,
                                      db_session, input_files):
    # given
    opening_file_path = tmp_path / 'no-id.csv'
    with open(opening_file_path, 'w') as f:
        fieldnames = sample_data_opening['fieldnames']
        entries = sample_data_opening['entries']
        entries[3]['unit'] = ''
        writer = csv.DictWriter(f,
                                fieldnames=fieldnames,
                                delimiter=';')

        writer.writeheader()
        for entry in entries:
            row = dict(ID=entry['id'],
                       DATA_INICIO=entry['initial_date'],
                       NOME=entry['name'],
                       NOTA=entry['note'],
                       UNIDADE=entry['unit'])
            writer.writerow(row)

    base_compact_configuration.opening_file_path = opening_file_path
    compact = Compact(base_compact_configuration)

    # when
    try:
        compact.compact()
    except Exception as e:
        assert e is None, 'No exceptions should be thrown'

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == opening_file_path, \
        'Wrong opening file path on database'
    assert opening_action.status == BROKEN_FILE, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == input_files['closing']['path'], \
        'Wrong closing file path on database'
    assert closing_action.status == OK, \
        'Wrong status saved on database'


def test_compact_closing_invalid_id(tmp_path, sample_data_closing,
                                    base_compact_configuration,
                                    db_session, input_files):
    # given
    closing_file_path = tmp_path / 'no-id.csv'
    with open(closing_file_path, 'w') as f:
        fieldnames = sample_data_closing['fieldnames']
        entries = sample_data_closing['entries']
        entries[2]['id'] = ''
        writer = csv.DictWriter(f,
                                fieldnames=fieldnames,
                                delimiter=';')

        writer.writeheader()
        for entry in entries:
            row = dict(ID=entry['id'],
                       DATA_FIM=entry['final_date'],
                       VALOR=entry['value'])
            writer.writerow(row)

    base_compact_configuration.closing_file_path = closing_file_path
    compact = Compact(base_compact_configuration)

    # when
    try:
        compact.compact()
    except Exception as e:
        assert e is None, 'No exceptions should be thrown'

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == input_files['opening']['path'], \
        'Wrong opening file path on database'
    assert opening_action.status == OK, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == closing_file_path, \
        'Wrong closing file path on database'
    assert closing_action.status == BROKEN_FILE, \
        'Wrong status saved on database'


def test_compact_closing_invalid_final_date(tmp_path, sample_data_closing,
                                            base_compact_configuration,
                                            db_session, input_files):
    # given
    closing_file_path = tmp_path / 'no-id.csv'
    with open(closing_file_path, 'w') as f:
        fieldnames = sample_data_closing['fieldnames']
        entries = sample_data_closing['entries']
        entries[2]['final_date'] = ''
        writer = csv.DictWriter(f,
                                fieldnames=fieldnames,
                                delimiter=';')

        writer.writeheader()
        for entry in entries:
            row = dict(ID=entry['id'],
                       DATA_FIM=entry['final_date'],
                       VALOR=entry['value'])
            writer.writerow(row)

    base_compact_configuration.closing_file_path = closing_file_path
    compact = Compact(base_compact_configuration)

    # when
    try:
        compact.compact()
    except Exception as e:
        assert e is None, 'No exceptions should be thrown'

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == input_files['opening']['path'], \
        'Wrong opening file path on database'
    assert opening_action.status == OK, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == closing_file_path, \
        'Wrong closing file path on database'
    assert closing_action.status == BROKEN_FILE, \
        'Wrong status saved on database'


def test_compact_closing_invalid_value(tmp_path, sample_data_closing,
                                       base_compact_configuration,
                                       db_session, input_files):
    # given
    closing_file_path = tmp_path / 'no-id.csv'
    with open(closing_file_path, 'w') as f:
        fieldnames = sample_data_closing['fieldnames']
        entries = sample_data_closing['entries']
        entries[2]['value'] = ''
        writer = csv.DictWriter(f,
                                fieldnames=fieldnames,
                                delimiter=';')

        writer.writeheader()
        for entry in entries:
            row = dict(ID=entry['id'],
                       DATA_FIM=entry['final_date'],
                       VALOR=entry['value'])
            writer.writerow(row)

    base_compact_configuration.closing_file_path = closing_file_path
    compact = Compact(base_compact_configuration)

    # when
    try:
        compact.compact()
    except Exception as e:
        assert e is None, 'No exceptions should be thrown'

    # then
    opening_action = db_session.query(FileParsing)\
        .filter_by(file_type=OPENING)\
        .one_or_none()
    assert opening_action is not None, 'Opening action not saved on database'
    assert opening_action.creation_date is not None, \
        'Creation date not saved on database'
    assert opening_action.file_path == input_files['opening']['path'], \
        'Wrong opening file path on database'
    assert opening_action.status == OK, \
        'Wrong status saved on database'

    closing_action = db_session.query(FileParsing)\
        .filter_by(file_type=CLOSING)\
        .one_or_none()
    assert closing_action is not None, 'Closing action not saved on database'
    assert closing_action.creation_date is not None, \
        'Creation date not saved on database'
    assert closing_action.file_path == closing_file_path, \
        'Wrong closing file path on database'
    assert closing_action.status == BROKEN_FILE, \
        'Wrong status saved on database'
