from setuptools import setup, find_packages

setup(
    name="data_engineering",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pyspark',
        'pandas',
        'numpy',
        'faker',
        'openpyxl'
    ],
    python_requires='>=3.10',
)
