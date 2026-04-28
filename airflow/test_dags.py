import os
import unittest

try:
    from airflow.models import DagBag
except Exception:  # pragma: no cover - optional dependency in local env
    DagBag = None

class TestDagIntegrity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if DagBag is None:
            raise unittest.SkipTest("apache-airflow is not installed")

    def setUp(self):
        # Point to the dags folder relative to this file
        dag_path = os.path.join(os.path.dirname(__file__), 'dags')
        self.dagbag = DagBag(dag_folder=dag_path, include_examples=False)

    def test_import_dags(self):
        """Test that all DAGs in the dags folder can be imported without errors."""
        self.assertFalse(
            len(self.dagbag.import_errors),
            f"DAG import failures: {self.dagbag.import_errors}"
        )

if __name__ == "__main__":
    unittest.main()
