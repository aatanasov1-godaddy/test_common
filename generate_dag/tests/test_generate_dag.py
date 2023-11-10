from glob import glob
from unittest import TestCase
from unittest.mock import MagicMock, patch

import yaml

from generate_dag.generate_dag import _create_dag_code

VALIDATE_CONFIG_PATH = 'generate_dag/validate_config.yaml'
REPORT_PATH = '/tmp/validation_report.yaml'


def read_validate_config():
    with open(VALIDATE_CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)


class TestValidateDags(TestCase):

    def setUp(self) -> None:
        self.report = {
            'summary': {
                'ok': 0,
                'error': 0,
                'skipped': 0
            },
            'passed': [],
            'failed': [],
            'skipped': [],
        }
        self.dags = self.get_dags()
        self.maxDiff = None

    def tearDown(self) -> None:
        self.report['summary']['ok'] = len(self.report['passed'])
        self.report['summary']['error'] = len(self.report['failed'])
        self.report['summary']['skipped'] = len(self.report['skipped'])
        with open(REPORT_PATH, 'w') as yaml_file:
            yaml.dump(self.report, yaml_file, default_flow_style=False, sort_keys=False)

    def get_dags(self):
        dag_files = {}
        for full_path in glob('**/dags/*.py', recursive=True):
            dag_name = full_path.split('/')[0]
            if dag_name not in read_validate_config().get('exclude', []):
                dag_files[dag_name] = full_path
            else:
                self.report['skipped'].append(dag_name)
        return dag_files

    def test_validate_dag_code(self):
        mock_open_calls = MagicMock()
        for dag in self.dags:
            with self.subTest(msg=f'Checking DAG code for {dag}'):
                with open(self.dags[dag], 'r') as f:
                    actual = f.read()
                with patch('generate_dag.generate_dag._write_file', mock_open_calls) as m:
                    expected = _create_dag_code(dag)
                    try:
                        self.assertMultiLineEqual(actual, expected)
                        self.report['passed'].append(dag)
                    except AssertionError as e:
                        self.report['failed'].append(dag)
                        raise AssertionError(e)

