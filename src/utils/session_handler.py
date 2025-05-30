from pyspark.sql import SparkSession
import os


class SessionBuilder:
    def __init__(self, app_name: str='data_engineer',
                 master: str="local[*]",
                 python_path: str=None) -> None:
        self.app_name = app_name
        self.master = master
        self.python_path = python_path or os.environ.get("PYSPARK_PYTHON", "")
        self.spark = None

    
    def build(self) -> SparkSession:
        if self.python_path and os.path.exists(self.python_path):
            os.environ["PYSPARK_PYTHON"] = self.python_path
            os.environ["PYSPARK_DRIVER_PYTHON"] = self.python_path
        elif self.python_path:
            raise FileNotFoundError(f"L'exécutable Python n'a pas été trouvé à : {self.python_path}")
        

        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.master) \
            .getOrCreate()
        
        return self.spark


    def stop(self) -> None:
        if self.spark:
            self.spark.stop()
