
from setuptools import setup, find_packages

# Obtain the long description from README.md
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='ak_data_collector',
    version='0.0.1',

    packages=find_packages(),

    install_requires=[
        'apache-airflow[postgres]',
        'pandas',
        'akshare'
    ],

   
    author='poolwsh',
    author_email='poolwsh@163.com',
    description='collect ak data.',
    long_description_content_type='text/markdown',
    url='https://github.com/poolwsh/ak_data_collector',
    
    include_package_data=False,

    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],

    python_requires='>=3.10',
)
