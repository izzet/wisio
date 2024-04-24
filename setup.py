# import subprocess
# from os import path
from pathlib import Path
from setuptools import find_packages, setup
# from skbuild import setup
# from skbuild.command.build import build
# from skbuild.command.install import install
# from skbuild.command.egg_info import egg_info


# def update_submodules():
#     print("Updating submodules...")
#     if path.exists('.gitmodules'):
#         subprocess.check_call(['git', 'submodule', 'init'])
#         subprocess.check_call(['git', 'submodule', 'update'])


# class build_with_submodules(build):
#     def run(self):
#         update_submodules()
#         build.run(self)


# class egg_info_with_submodules(egg_info):
#     def run(self):
#         update_submodules()
#         egg_info.run(self)


# class install_with_submodules(install):
#     def run(self):
#         update_submodules()
#         install.run(self)


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
    long_description=(Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
    python_requires=">=3.8, <4",
    # cmake_source_dir="tools",
    # cmdclass={
    #     "build": build_with_submodules,
    #     "egg_info": egg_info_with_submodules,
    #     "install": install_with_submodules,
    # },
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
