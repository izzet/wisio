import pathlib
from setuptools import setup, find_packages

setup(
    name='wisio',
    version='0.0.1',
    url='https://github.com/izzet/wisio',
    author='Izzet Yildirim',
    author_email='iyildirim@hawk.iit.edu',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    packages=find_packages(),
    python_requires=">=3.8, <4",
    entry_points={
        'console_scripts': [
            'wisio=wisio.__main__:main',
        ]
    },
    install_requires=[
        'click==8.0.4',
        'darshan>=3.4.0',
        'dask>=2023.9.0',
        'dask_jobqueue==0.8.2',
        'distributed>=2023.9.0',
        'fastparquet>=2023.10.0',
        'inflect==7.0.0',
        'jinja2>=3.0',
        'numpy==1.24.3',
        'matplotlib==3.2.1',
        'pandas>=2.1.0',
        'scipy>=1.10.0',
    ],
)
