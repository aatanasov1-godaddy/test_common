import argparse
import json
import logging
import os
import re
import shutil

import yaml

CONFIG_DIR = os.path.join('generate_dag', 'dag_config')
DAG_TEMPLATES = os.path.join('generate_dag', 'templates')
GLOBAL_CONFIG = os.path.join('generate_dag', 'dag_config', 'config.yaml')


FOLDER_STRUCTURE = [
    '',
    'dags',
    'spark',
    'spark/sql'
]

DDLs = [
    'history',
    'main'
]

SPARK_FILES = [
    'history'
]

CODE_PATH_START = 'gdcorp-dna/de-marketing-mdm/'


def _clean(dag_name):
    logging.info(f'Start cleaning {dag_name}')
    shutil.rmtree(dag_name)
    logging.info(f'Cleaning of {dag_name} completed successfully')


def _format_dependencies(dependencies):
    return ',\n    '.join([f"'{dependency}'" for dependency in dependencies])


def _format_bootstrap(bootstrap_path):
    return '{CODE_PATH}/' + f"{bootstrap_path}"


def _read_yaml_file(file_name):
    with open(file_name, 'r') as file_content:
        try:
            return yaml.safe_load(file_content)
        except yaml.YAMLError as e:
            logging.error(f'Unable to read config file - {file_name}')
            raise yaml.YAMLError(e)


def _read_base_config():
    if os.path.exists(GLOBAL_CONFIG):
        return _read_yaml_file(GLOBAL_CONFIG)
    logging.info(f'No global config found {GLOBAL_CONFIG}')


def _write_file(file_path, content):
    with open(file_path, 'w') as f:
        f.write(content)


def _get_config(key, yaml_config, global_config):
    if yaml_config and key in yaml_config:
        return yaml_config[key]
    if global_config and key in global_config:
        return global_config[key]
    return ''


def _build_config(dag_name, global_config):
    yaml_config = _read_yaml_file(f'{CONFIG_DIR}/{dag_name}.yaml')
    try:
        return {
            'base': {
                'pipeline_name': yaml_config['pipeline_name'],
                'data_lake_table_name': yaml_config.get('data_lake_table_name', yaml_config['pipeline_name']),
                'interval': yaml_config['interval'],
                'airflow_tag_layer': yaml_config['airflow_tag']['layer'],
                'airflow_tag_team': yaml_config['airflow_tag']['team'],
                'dependencies': _format_dependencies(yaml_config['dependencies']),
                'master_instance_type': yaml_config['emr']['master_instance_type'],
                'core_instance_type': yaml_config['emr']['core_instance_type'],
                'number_of_core_instances': yaml_config['emr']['number_of_core_instances'],
                'stg_waiter_poke_interval': yaml_config['emr'].get('stg_waiter_poke_interval', 60),
                'stg_waiter_poke_max_attempts': yaml_config['emr'].get('stg_waiter_poke_max_attempts', 120),
                'main_waiter_poke_interval': yaml_config['emr'].get('main_waiter_poke_interval', 60),
                'main_waiter_poke_max_attempts': yaml_config['emr'].get('main_waiter_poke_max_attempts', 120),
                'extract_waiter_poke_interval': yaml_config['emr'].get('extract_waiter_poke_interval', 60),
                'extract_waiter_poke_max_attempts': yaml_config['emr'].get('extract_waiter_poke_max_attempts', 120),
                'bootstrap_file_path': _format_bootstrap(yaml_config['emr']['bootstrap_file_path']),
                'alerts_slack': _get_config('alerts_slack', yaml_config, global_config),
                'confluence_link': _get_config('confluence_link', yaml_config, global_config),
                'data_domain': _get_config('data_domain', yaml_config, global_config),
                'data_sub_domain': yaml_config['emr']['emr_tag']['data_sub_domain'],
                'data_tier': _get_config('data_tier', yaml_config, global_config),
                'dependency_for': _get_config('dependency_for', yaml_config, global_config),
                'detailed_runbook_link': _get_config('detailed_runbook_link', yaml_config, global_config),
                'dev_group_slack': _get_config('dev_group_slack', yaml_config, global_config),
                'on_call_group': _get_config('on_call_group', yaml_config, global_config),
                'oncall_group_email': _get_config('oncall_group_email', yaml_config, global_config),
                'oncall_group_slack': _get_config('oncall_group_slack', yaml_config, global_config),
                'purpose': _get_config('purpose', yaml_config, global_config),
                'sla': _get_config('sla', yaml_config, global_config),
                'stake_holders': _get_config('stake_holders', yaml_config, global_config),
                'team_name': _get_config('team_name', yaml_config, global_config),
                'team_slack_channel': _get_config('team_slack_channel', yaml_config, global_config),
            },
            'commands': {
                'ingest_data_stg': yaml_config['emr']['ingest_data_stg'],
                'ingest_data': yaml_config['emr']['ingest_data'],
                'extract_data': yaml_config['emr']['extract_data'],
            }
        }
    except KeyError as e:
        logging.error(f'Not valid configuration in the dag config - {CONFIG_DIR}/{dag_name}.yaml')
        raise KeyError(e)


def _validate_file_names(dag_names):
    if dag_names is None:
        return True
    else:
        for dag_name in dag_names:
            file_name = f'{dag_name}.yaml'
            if not os.path.isfile(os.path.join(CONFIG_DIR, file_name)):
                logging.error(f"{file_name} does not exist.")
                return False
    return True


def _create_folder_structure(dag_name):
    logging.info('Start creating folder structure')
    for folder in FOLDER_STRUCTURE:
        folder_path = f'{dag_name}/{folder}'
        os.mkdir(folder_path)
        logging.info(f'Folder - {folder_path} created')
    logging.info('Folders created successfully')


def _create_ddl_files(dag_name):
    for ddl_type in DDLs:
        logging.info(f'Start creating ddl - {ddl_type}')
        ddl_template = f'{DAG_TEMPLATES}/ddl_{ddl_type}.ddl'
        if ddl_type == 'main':
            ddl = f'{dag_name}/spark/sql/{dag_name}.ddl'
        elif ddl_type == 'history':
            ddl = f'{dag_name}/spark/sql/{dag_name}_history.ddl'
        else:
            logging.warning(f'Unrecognised ddl_type - {ddl_type}. Skipping')
            continue
        with open(ddl_template, 'r') as ddl_template_file:
            ddl_content = ddl_template_file.read().replace('<dag_name>', dag_name)
        _write_file(ddl, ddl_content)
        logging.info(f'ddl - {ddl_type} created successfully')


def _create_spark_files(dag_name):
    for spark_file_type in SPARK_FILES:
        logging.info(f'Start creating spark file - {spark_file_type}')
        if spark_file_type == 'history':
            shutil.copyfile(f'{DAG_TEMPLATES}/spark_{spark_file_type}.py', f'{dag_name}/spark/{dag_name}_history.py')
        else:
            logging.warning(f'Unrecognised spark type file - {spark_file_type}. Skipping')
        logging.info(f'Spark file - {spark_file_type} created successfully')


def _format_multiline_string(inp_str, indentation=0, has_variables=True):
    multiline_string = ''
    inp_lst = inp_str.split()
    multiline_list = []
    is_property = False
    for i in range(len(inp_lst)):
        current_line = f'{inp_lst[i]}'
        if is_property:
            multiline_list[-1] += f' {current_line}'
        else:
            multiline_list.append(f'{current_line}')
        is_property = inp_lst[i].startswith('-')
    for i in range(len(multiline_list)):
        line = multiline_list[i]
        start_quote = "f'" if '{' in line and has_variables else "'"
        end_space = ' ' if i != len(multiline_list) - 1 else ''
        multiline_string += f"{' ' * (indentation+6)}{start_quote}{line}{end_space}'\n"
    return f"(\n{multiline_string}{' '* (indentation+5) })"


def _optimise_command_templates(dag_content, ingest_data_stg, ingest_data, extract_data):
    base_spark_command = ['spark-submit', '--deploy-mode client', '--master yarn']
    commands = {
        'ingest_data_stg_cmd': " {CODE_PATH}/{DAG_ID}/spark/{DAG_ID}_history.py " + ingest_data_stg['params'] +
                               " -e {ENV}",
        'ingest_data_main_cmd': " {CODE_PATH}/utils/spark/ingest_data_main.py " + ingest_data['params'] +
                               " -e {ENV}",
        'extract_data_cmd': " {CODE_PATH}/utils/spark/extract_data.py " + extract_data['params'] +
                               " -e {ENV} -l {LOAD_TYPE} -dc {shlex.quote(DAG_CONFIG)}",
    }
    if 'custom_rename' in extract_data:
        custom_rename_value = _format_multiline_string(json.dumps(extract_data['custom_rename']), 11, False)
        custom_rename = f"CUSTOM_RENAME = {custom_rename_value}"
        commands['extract_data_cmd'] += ' -cr {shlex.quote(CUSTOM_RENAME)}'
        if extract_data.get('final_rename', False):
            commands['extract_data_cmd'] += ' --final-rename'
    else:
        custom_rename = ''
    if 'custom_transformation' in extract_data:
        custom_transformation_value = _format_multiline_string(json.dumps(extract_data['custom_transformation']), 19,
                                                               False)
        custom_transformation = f"CUSTOM_TRANSFORMATION = {custom_transformation_value}"
        commands['extract_data_cmd'] += ' -ct {shlex.quote(CUSTOM_TRANSFORMATION)}'
    else:
        custom_transformation = ''
    py_files = []
    for elem in ingest_data_stg['py_files']:
        if elem.startswith(CODE_PATH_START):
            py_files.append('{CODE_PATH}/' + elem.replace(CODE_PATH_START, ''))
        else:
            py_files.append('{CODE_BUCKET}/' + elem)
    if ingest_data_stg['py_files'] == ingest_data['py_files'] == extract_data['py_files']:
        dag_content = dag_content.replace('<py_files>', f"PY_FILES = f'--py-files {','.join(py_files)}'")
        for command in commands:
            commands[command] = ' {PY_FILES}' + commands[command]
    else:
        dag_content = dag_content.replace('<py_files>\n', '')
        commands['ingest_data_stg_cmd'] = ' --py-files ' + ','.join(py_files) + commands['ingest_data_stg_cmd']
        commands['ingest_data_main_cmd'] = ' --py-files ' + ','.join(py_files) + commands['ingest_data_main_cmd']
        commands['extract_data_cmd'] = ' --py-files ' + ','.join(py_files) + commands['extract_data_cmd']

    if ingest_data_stg['spark_cmd'] == ingest_data['spark_cmd'] == extract_data['spark_cmd']:
        replace_with = "BASE_SPARK_COMMAND = "
        replace_with += _format_multiline_string(' '.join(base_spark_command + [ingest_data_stg['spark_cmd']]), 16)
        dag_content = dag_content.replace('<spark_cmd>', replace_with)
        for command in commands:
            commands[command] = _format_multiline_string("{BASE_SPARK_COMMAND}" + commands[command], 12)
    else:
        dag_content = dag_content.replace('<spark_cmd>\n', '')
        commands['ingest_data_stg_cmd'] = _format_multiline_string(
            f"{' '.join(base_spark_command)} {ingest_data_stg['spark_cmd']}{commands['ingest_data_stg_cmd']}", 12
        )
        commands['ingest_data_main_cmd'] = _format_multiline_string(
            f"{' '.join(base_spark_command)} {ingest_data['spark_cmd']}{commands['ingest_data_main_cmd']}", 12
        )
        commands['extract_data_cmd'] = _format_multiline_string(
            f"{' '.join(base_spark_command)} {extract_data['spark_cmd']}{commands['extract_data_cmd']}", 12
        )
    dag_content = dag_content.replace('<ingest_data_stg_cmd>', commands['ingest_data_stg_cmd'])
    dag_content = dag_content.replace('<ingest_data_main_cmd>', commands['ingest_data_main_cmd'])
    dag_content = dag_content.replace('<extract_data_cmd>', commands['extract_data_cmd'])
    dag_content = dag_content.replace('<custom_rename>', custom_rename)
    dag_content = dag_content.replace('<custom_transformation>', custom_transformation)
    return dag_content


def _create_dag_code(dag_name):
    global_config = _read_base_config()
    dag_config = _build_config(dag_name, global_config)
    with open(f'{DAG_TEMPLATES}/dag_template.tmpl', 'r') as dag_template:
        dag_content = dag_template.read()
    for key in dag_config['base']:
        dag_content = dag_content.replace(f'<{key}>', str(dag_config['base'][key]))
    dag_content = _optimise_command_templates(dag_content, dag_config['commands']['ingest_data_stg'],
                                              dag_config['commands']['ingest_data'],
                                              dag_config['commands']['extract_data'])
    #: Remove any trailing spaces at the end of each line
    dag_content_strip = re.sub(r' ([\n|\r\n])', r'\1', dag_content)
    _write_file(f'{dag_name}/dags/{dag_name}.py', dag_content_strip)
    return dag_content_strip


def _create_files(dag_name):
    _create_ddl_files(dag_name)
    _create_spark_files(dag_name)
    _create_dag_code(dag_name)


def _regenerate(dag_name, overwrite):
    if overwrite:
        _create_dag_code(dag_name)
        return
    is_sure = input(f'Are you sure you want to overwrite the DAG file for {dag_name}? (y/n): ')
    if is_sure.lower() == 'y':
        _create_dag_code(dag_name)
    elif is_sure.lower() == 'n':
        logging.info('Closing the application.')
    else:
        _regenerate(dag_name, overwrite)


def main(dag_names, overwrite, clean):
    if not _validate_file_names(dag_names):
        return
    for dag_name in dag_names:
        is_path_exists = os.path.exists(dag_name)
        if clean and is_path_exists:
            _clean(dag_name)
        elif clean and not is_path_exists:
            logging.warning(f"Path - {dag_name} doesn't exists. Cleaning Skipped!")
        elif is_path_exists:
            logging.warning(f'Path - {dag_name} already exists.')
            _regenerate(dag_name, overwrite)
        else:
            _create_folder_structure(dag_name)
            _create_files(dag_name)


if __name__ == "__main__":
    logging.basicConfig(level='INFO', format='%(asctime)s | %(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(description='Create airflow pipelines from the config files')
    parser.add_argument('-d', '--dag-names', nargs='*', help='Name of the config files separated by space')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Overwrite DAG code')
    parser.add_argument('-c', '--clean', action='store_true', help='Clean directory structure')
    args = parser.parse_args()
    main(args.dag_names, args.overwrite, args.clean)
