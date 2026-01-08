from setuptools import setup, find_packages

setup(
    name="api_ingestion",
    version="0.1.0",
    description="Spark Data Source for reading from a REST API",
    author="Filip Wagner",
    author_email="filip.wagner@takeda.com",
    packages=find_packages(),
    install_requires=[
        "requests>=2.0"
    ]
)