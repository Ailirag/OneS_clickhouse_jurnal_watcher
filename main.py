import json
import re
import subprocess
import sys
import os
import time
import traceback

from secure_json import Settings as Settingsv2
import requests
import shutil
from datetime import datetime, timedelta
import zipfile

settingsv2 = Settingsv2('settings.json').data


class LOGS_TABLES:
    tables_str = 'system.asynchronous_metric_log, ' \
                 'system.metric_log,' \
                 'system.part_log,' \
                 'system.query_log,' \
                 'system.query_thread_log,' \
                 'system.trace_log,' \
                 'system.session_log'
    tables = tables_str.split(',')


def cmd_get_result(cmd):
    try:
        output = subprocess.check_output(cmd)
        decoded_output = output.decode('CP866')
        return decoded_output
    except Exception as e:
        return None


def paths_from_lst():
    output = cmd_get_result(f'sc query')
    if output is None:
        raise Exception('Error')
    ru = output.find('Имя_службы') > -1
    if ru:
        pattern = "Имя_службы: (1C:.+)"
    else:
        pattern = "SERVICE_NAME: (1C:.+)"

    pattern_uid = '{([\w0-9]{8}-[\w0-9]{4}-[\w0-9]{4}-[\w0-9]{4}-[\w0-9]{12}),"(.*?)"'

    all_paths = dict()

    all_find = re.findall(pattern, output)
    for finded in all_find:
        output = cmd_get_result(f'sc qc "{finded[:-1]}"')
        if ru:
            pattern = 'Имя_двоичного_файла.+ {1,2}-regport {1,2}([\d]+).+-port {1,2}([\d]+) {1,2}.+-d {1,2}"(.+)"'
        else:
            pattern = 'BINARY_PATH_NAME.+ {1,2}-regport {1,2}([\d]+).+-port {1,2}([\d]+) .+-d {1,2}"(.+)"'

        re_data = re.findall(pattern, output)
        if len(re_data) == 0:
            continue
        all_data = re_data[0]
        regport = all_data[0]
        port = all_data[1]
        srvinfo_path = all_data[2]
        mod_srvinfo_path = f'{srvinfo_path[:1]}:{srvinfo_path[2:]}{os.sep}reg_{regport}{os.sep}1CV8Clst.lst'
        with open(mod_srvinfo_path, 'r', encoding='utf8') as file:
            all_text = file.read()

        all_find_uid = re.findall(pattern_uid, all_text)
        all_paths[regport] = dict()
        for i in all_find_uid:
            all_paths[regport][i[1]] = srvinfo_path + os.sep + 'reg_' + regport + os.sep + i[0] + os.sep + '1Cv8Log'

    return all_paths


def logging(text):
    with open(f'{os.getcwd() + os.sep}log.txt', 'a', encoding='utf8') as log_file:
        for log_text in text.split('\n'):
            message = f'{datetime.now()} :   {log_text}'
            print(message)
            log_file.write(f'{message}\n')


def date_serialization(file_name):
    mas_name = file_name.split('.')
    if len(mas_name) > 2 or len(mas_name) == 1:
        return datetime(3999, 12, 31)
    if mas_name[1][:3] == 'lgp' or mas_name[1][:3] == 'lgx':
        date_in_name = mas_name[0]
        loc_year = int(date_in_name[:4])
        loc_month = int(date_in_name[4:6])
        loc_day = int(date_in_name[6:8])
        loc_hour = int(date_in_name[8:10])
        loc_minutes = int(date_in_name[10:12])
        serialized = datetime(loc_year, loc_month, loc_day, loc_hour, loc_minutes)
        return serialized
    else:
        return datetime(3999, 12, 31)


def archiving_v8logs(file_name, path_to_v8logs):
    try:
        logging(f'archiving date {file_name[:-4]}')
        name_archive = f'{path_to_v8logs}{os.sep}{settingsv2.cluster_base_name}_{file_name[:-4]}.zip'
        name_backup = f'{settingsv2.backup_path}{os.sep}{settingsv2.cluster_base_name}_{file_name[:-4]}.zip'
        with zipfile.ZipFile(name_archive, 'w') as myzip:
            name_lgx = f'{path_to_v8logs}{os.sep}{file_name[:-4]}.lgx'
            if os.path.exists(name_lgx):
                myzip.write(name_lgx, arcname=f'{file_name[:-4]}.lgx',
                            compress_type=zipfile.ZIP_DEFLATED,
                            compresslevel=7)
            myzip.write(f'{path_to_v8logs}{os.sep}{file_name}', arcname=file_name,
                        compress_type=zipfile.ZIP_DEFLATED, compresslevel=7)
            myzip.write(f'{path_to_v8logs}{os.sep}1Cv8.lgf', arcname='1Cv8.lgf',
                        compress_type=zipfile.ZIP_DEFLATED, compresslevel=7)

        logging(f'Deleting {file_name} and {file_name[:-3]}lgx')

        delete1 = False
        delete2 = False

        while True:
            try:
                if os.path.exists(f'{path_to_v8logs}{os.sep}{file_name}'):
                    os.remove(f'{path_to_v8logs}{os.sep}{file_name}')
                    delete1 = True
                if os.path.exists(f'{path_to_v8logs}{os.sep}{file_name[:-3]}lgx'):
                    os.remove(f'{path_to_v8logs}{os.sep}{file_name[:-3]}lgx')
                    delete2 = True
                if delete1 and delete2:
                    break
            except Exception as e:
                logging('Failed delete to delete log file. ' + str(type(e)) + str(e))
                logging('Pause 300 seconds to retry.')
                time.sleep(300)

        logging(f'Try to move in repo: {settingsv2.backup_path}')
        shutil.move(name_archive, name_backup)

        logging('Success')
    except Exception as e:
        error_exc = str(type(e)) + str(e)
        logging(f'Action failed with an error: {error_exc}')


def archiving_data_update():
    result = clickhouse_query(f'''CREATE TABLE IF NOT EXISTS {settingsv2.clickhouse.database_name}.HistoryChangesEventLog
                (
                    Id Int64 Codec(DoubleDelta, LZ4),
                    DateTime DateTime('UTC') Codec(Delta, LZ4),
                    User LowCardinality(String),
                    User_AD LowCardinality(String),
                    Computer LowCardinality(String),
                    Application LowCardinality(String),
                    Connection Int64 Codec(DoubleDelta, LZ4),
                    Event LowCardinality(String),
                    Metadata LowCardinality(String),
                    DataPresentation String Codec(ZSTD),
                    Session Int64 Codec(DoubleDelta, LZ4)
                )
                engine = MergeTree()
                PARTITION BY (toYYYYMM(DateTime))
                ORDER BY (DateTime)
                SETTINGS index_granularity = 8192;''')

    if result.status_code != 200:
        logging('Creating table HistoryChangesEventLog failed.')
        return

    data = clickhouse_query(f'''select toStartOfDay(DateTime) as DateTime from {settingsv2.clickhouse.database_name}.EventLogItems
                            WHERE toStartOfDay(DateTime) BETWEEN date_add(day, -1, toStartOfDay(now()))  AND date_add(second, 86399, date_add(day, -1, toStartOfDay(now())))
                            limit 1
                            FORMAT JSON''')
    if data.status_code == 200:
        result_json = json.loads(data.text)
        if result_json['data']:
            clearing_date = result_json['data'][0]['DateTime']
            clearing_date = datetime.strptime(clearing_date, '%Y-%m-%d %H:%M:%S')
        else:
            clearing_date = datetime(1999, 12, 31)

        data = clickhouse_query(f'''select toStartOfDay(DateTime) as DateTime from {settingsv2.clickhouse.database_name}.HistoryChangesEventLog
                            WHERE toStartOfDay(DateTime) BETWEEN date_add(day, -1, toStartOfDay(now()))  AND date_add(second, 86399, date_add(day, -1, toStartOfDay(now())))
                            limit 1
                            FORMAT JSON''')

        if data.status_code == 200:
            result_json = json.loads(data.text)
            if result_json['data']:
                HistoryDate = result_json['data'][0]['DateTime']
                HistoryDate = datetime.strptime(HistoryDate, '%Y-%m-%d %H:%M:%S')
            else:
                HistoryDate = datetime(1998, 12, 31)
        else:
            logging(f'Getting data from {settingsv2.clickhouse.database_name}.HistoryChangesEventLog failed')
            return

        if clearing_date > HistoryDate:
            result = clickhouse_query(f'''
                    INSERT  
                    INTO {settingsv2.clickhouse.database_name}.HistoryChangesEventLog  
                    (Id, DateTime, User, Computer, Application, Connection, Event, Metadata, DataPresentation, Session, User_AD) 
                        SELECT * FROM 
                            (SELECT Id, DateTime, User, Computer, Application, Connection, Event, Metadata, DataPresentation, Session, Data_AD.Data as User_AD  FROM {settingsv2.clickhouse.database_name}.EventLogItems AS  {settingsv2.clickhouse.database_name}
                                LEFT JOIN ( SELECT distinct
                                            User, Data
                                            FROM {settingsv2.clickhouse.database_name}.EventLogItems
                                            WHERE toStartOfDay(DateTime) BETWEEN date_add(day, -2, toStartOfDay(now()))  AND date_add(second, 86399, date_add(day, -1, toStartOfDay(now()))) 
                                            AND Event = 'Сеанс.Аутентификация' AND notEmpty(Data) = 1 ) AS Data_AD
                                ON EventLogItems.User = Data_AD.User
                                 WHERE Event = 'Данные.Изменение'
                                 AND notEmpty({settingsv2.clickhouse.database_name}.DataPresentation) = 1 AND {settingsv2.clickhouse.database_name}.DateTime  BETWEEN date_add(day, -1, toStartOfDay(now()))  AND date_add(second, 86399, date_add(day, -1, toStartOfDay(now())))
                                ) AS DataForDay''')

            if result.status_code != 200:
                logging('Insert data to HistoryChangesEventLog failed.')

    else:
        logging(f'Getting data from {settingsv2.clickhouse.database_name}.EventLogItems failed')



def start_mutations_on_clickhouse(date_border):
    count_days = timedelta(days=settingsv2.clickhouse.deep_of_history)
    cleaning_border = date_border - count_days

    if len(str(cleaning_border.month)) == 1:
        month = f'0{cleaning_border.month}'
    else:
        month = cleaning_border.month

    if len(str(cleaning_border.day)) == 1:
        day = f'0{cleaning_border.day}'
    else:
        day = cleaning_border.day

    archiving_data_update()

    cleaning_border_str = f'{cleaning_border.year}{month}{day}230000'
    result = clickhouse_query(
        f"alter table {settingsv2.clickhouse.database_name}.EventLogItems DELETE WHERE FileName < '{cleaning_border_str}.lgp'")
    if result.status_code == 200:
        logging('Mutation on deleting data in clickhouse started.')

    if len(settingsv2.removal_conditions):
        logging('Start mutation to delete events on conditions.')

        for condition in settingsv2.removal_conditions:
            query = f'alter table {settingsv2.clickhouse.database_name}.EventLogItems DELETE WHERE DateTime > toStartOfDay(now()) - 86400 and DateTime < toStartOfDay(now()) and {condition}'
            result = clickhouse_query(query)

            if result.status_code != 200:
                logging(f'   Failed. {result.text}')
            else:
                logging('   Success.')

    logging('Clearing log tables...')
    try:
        for table in LOGS_TABLES.tables:
            result = clickhouse_query(f"truncate table {table}")
            if result.status_code != 200:
                logging(f'  table [{table}] not truncated. Response code = {result.status_code}')
        logging('   Success')
    except Exception as e:
        error_exc = str(type(e)) + str(e)
        logging(f'Cleaning log tables failed with an error: {error_exc}')


def send_message(text: str):
    url = "https://api.telegram.org/bot"
    url += settingsv2.telegram.bot_token
    method = url + "/sendMessage"

    r = requests.post(method, data={
        "chat_id": settingsv2.telegram.chat_id,
        "text": text
    })
    if r.status_code != 200:
        logging(f'chat_id={settingsv2.telegram.chat_id}, status: {r.text}')


def clickhouse_query(query_text, problem=None):
    headers = {'X-ClickHouse-User': f'{settingsv2.clickhouse.user}',
               'X-ClickHouse-Key': f'{settingsv2.clickhouse.password}'}
    try:
        return requests.request('POST', f'{settingsv2.clickhouse.url}', headers=headers, data=query_text.encode('utf-8'))
    except Exception as e:
        if not problem is None:
            logging(f'Error on clickhouse query. {str(type(e)) + str(e)}')
        return {
            'status_code': 404,
            'text': 'Error'
        }


def try_to_archive_and_clean(last_border, problem, path_to_v8logs):

    data = clickhouse_query(f"select max(FileName) from {settingsv2.clickhouse.database_name}.EventLogItems", problem)
    status_code = data.status_code

    if status_code != 200 and not problem:
        logging(f'Status code: {status_code}: {data.text}')
        send_message(f'{settingsv2.cluster_base_name}\nPROBLEM. Clickhouse: {settingsv2.clickhouse.database_name}.\nStatus code != 200.')
        return last_border, True

    if status_code == 200:
        date_border = date_serialization(data.text)
        if date_border == last_border:
            return date_border, False
        logging(f'Border file: [{date_border}].\nTrying to archiving earlier files')
        # lgp lgx
        mas_delete = []
        for file_name in os.listdir(f'{path_to_v8logs}'):
            if file_name[-3:] != 'lgp':
                continue
            date = date_serialization(file_name)
            if date < date_border:
                mas_delete.append(file_name)
        if mas_delete:
            for file_name in mas_delete:
                try:
                    archiving_v8logs(file_name, path_to_v8logs)
                except Exception as e:
                    error_exc = str(type(e)) + str(e)
                    logging(f'Error archiving/deleting file [{file_name}]   :   {error_exc}')
                    sys.exit()

            logging('All files successfully deleted.')
        else:
            logging('Files earlier than border not finded.')
        if settingsv2.clickhouse.deep_of_history:
            start_mutations_on_clickhouse(date_border)
        return date_border, False


def restart_service():
    logging('Try to restart service.')
    os.system(f'sc stop {settingsv2.service.name}')
    time.sleep(15)
    os.system(f'sc start {settingsv2.service.name}')


def stop_service():
    logging('Stopping service.')
    os.system(f'sc stop {settingsv2.service.name}')
    time.sleep(5)


def run_service():
    logging('Running service.')
    os.system(f'sc start {settingsv2.service.name}')
    time.sleep(10)


def check_new_file(last_date, path_to_v8logs):
    data = clickhouse_query(f"select max(FileName) from {settingsv2.clickhouse.database_name}.EventLogItems")
    status_code = data.status_code

    if status_code != 200:
        logging(f'Status code: {status_code}: {data.text}')
        send_message(f'{settingsv2.cluster_base_name}\nPROBLEM. Clickhouse: {settingsv2.clickhouse.url}.\nStatus code != 200.')
        return

    if status_code == 200:
        date_border = date_serialization(data.text)
        # lgp lgx
        all_dates = []
        for file_name in os.listdir(f'{path_to_v8logs}'):
            if file_name[-3:] != 'lgp':
                continue
            date = date_serialization(file_name)
            all_dates.append(date)

        file_border = max(all_dates)
        if file_border > date_border:
            stop_service()

            mas_delete = []
            for file_name in os.listdir(f'{path_to_v8logs}'):
                if file_name[-3:] != 'lgp':
                    continue
                date = date_serialization(file_name)
                if date < file_border:
                    mas_delete.append(file_name)
            if mas_delete:
                for file_name in mas_delete:
                    try:
                        archiving_v8logs(file_name, path_to_v8logs)
                    except Exception as e:
                        error_exc = str(type(e)) + str(e)
                        logging(f'Error archiving/deleting file [{file_name}]   :   {error_exc}')
                        sys.exit()

                logging('All files successfully deleted.')
            run_service()
            logging('New date updated.')
            if settingsv2.clickhouse.deep_of_history:
                start_mutations_on_clickhouse(date_border)
            return file_border
        else:
            return last_date


if __name__ == '__main__':

    try:

        date_border = datetime(1, 1, 1)
        last_datetime = ''
        last_value = ''
        waiting_time = settingsv2.service.data_waiting_time_sec
        count_restart_service = settingsv2.service.count_restart_service

        data_start_waiting = datetime.now()

        waiting_data = False
        problem_waiting_data = False

        problem = False

        current_count_restarts = 0

        paths = paths_from_lst()

        path_to_v8logs = ''

        for key, value in paths.items():
            if settingsv2.cluster_base_name in value and path_to_v8logs == '':
                path_to_v8logs = value[settingsv2.cluster_base_name]
            elif path_to_v8logs != '':
                base_error = 'База опубликована в нескольких одновременно работающих версиях платформы'
                send_message(f'{settingsv2.cluster_base_name}\nClickhouse journal watcher.\n{base_error}.\nФатальная ошибка.')
                logging(f'ERROR. {base_error}.')
                sys.exit()

        if path_to_v8logs == '':
            logging('ERROR. Path to v8logs not finded, please check your OneC cluster.')
            sys.exit()

        while True:

            date_border, problem = try_to_archive_and_clean(date_border, problem, path_to_v8logs)

            data = clickhouse_query(f'select max(DateTime) from {settingsv2.clickhouse.database_name}.EventLogItems')
            if data.status_code != 200 and not problem:
                text_problem = f'{settingsv2.cluster_base_name}\nPROBLEM. Clickhouse service check. Status code: {data.status_code}'
                send_message(text_problem)
                logging(text_problem)
                problem = True
                continue
            elif data.status_code == 200 and problem:
                text_problem = f'{settingsv2.cluster_base_name}\nOK. Clickhouse service check. Status code: 200'
                send_message(text_problem)
                logging(text_problem)
                problem = False

            if data.text == last_datetime:
                if not waiting_data:
                    logging('Start waiting data.')
                    data_start_waiting = datetime.now()
                    waiting_data = True
                else:
                    wait_border = datetime.now() - timedelta(seconds=waiting_time)
                    if data_start_waiting <= wait_border:
                        if current_count_restarts >= count_restart_service:
                            if not problem_waiting_data:
                                text_problem = f'{settingsv2.cluster_base_name}\nPROBLEM. Clickhouse service check.\nНет новых данных на протяжении {count_restart_service * waiting_time // 60} мин.'
                                send_message(text_problem)
                                logging(text_problem)
                                problem_waiting_data = True

                            new_last_datetime = check_new_file(last_datetime, path_to_v8logs)
                            if new_last_datetime == last_datetime:
                                restart_service()
                            else:
                                last_datetime = new_last_datetime
                        else:
                            waiting_data = False
                            current_count_restarts += 1
                            data_start_waiting = datetime.now()
                            restart_service()

            else:
                last_datetime = data.text
                waiting_data = False
                if current_count_restarts > 0:
                    current_count_restarts = 0
                if problem_waiting_data:
                    logging('OK. Data received')
                    problem_waiting_data = False
                    send_message(f'{settingsv2.cluster_base_name}\nOK. Clickhouse service check.\nДанные получены.')

            time.sleep(60)

    except Exception as e:
        error_exc = str(type(e)) + str(e)
        logging(error_exc)
        error_exc = traceback.format_exc()
        logging(error_exc)

