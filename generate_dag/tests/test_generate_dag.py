from glob import glob
from unittest import TestCase
from unittest.mock import MagicMock, patch

import yaml

from generate_dag.generate_dag import _create_dag_code

VALIDATE_CONFIG_PATH = 'generate_dag/validate_config.yaml'


def read_validate_config():
    with open(VALIDATE_CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)


def get_dags():
    dag_files = {}
    for full_path in glob('**/dags/*.py', recursive=True):
        dag_name = full_path.split('/')[0]
        if dag_name not in read_validate_config().get('exclude', []):
            dag_files[dag_name] = full_path
    return dag_files


class TestExportMetlFramework(TestCase):

    def setUp(self) -> None:
        self.dags = get_dags()
        self.maxDiff = None

    def test_validate_dag_code(self):
        mock_open_calls = MagicMock()
        for dag in self.dags:
            with self.subTest(msg=f'Checking DAG code for {dag}'):
                with open(self.dags[dag], 'r') as f:
                    actual = f.read()
                with patch('generate_dag.generate_dag._write_file', mock_open_calls) as m:
                    expected = _create_dag_code(dag)
                    self.assertMultiLineEqual(actual, expected)
