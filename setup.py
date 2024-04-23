from setuptools import find_packages
from skbuild import setup

setup(
    name="wisio",
    version="0.0.1",
    url="https://github.com/izzet/wisio",
    author="Izzet Yildirim",
    author_email="izzetcyildirim@gmail.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8, <4",
    cmake_source_dir="tools",
    entry_points={
        "console_scripts": [
            "wisio=wisio.__main__:main",
        ]
    },
    packages=find_packages(),
    install_requires=[
        "dask>=2023.9.0",
        "dask_jobqueue==0.8.2",
        "distributed>=2023.9.0",
        "inflect==7.0",
        "jinja2>=3.0",
        "numpy==1.24.3",
        "matplotlib==3.2.1",
        "pandas>=2.1",
        "pyyaml>=5.4",
        "rich==13.6.0",
        "scikit-learn>=1.4",
        "scipy>=1.10",
        "venn==0.1.3",
    ],
    extras_require={"darshan": ["darshan>=3.4"]},
)
