<p align="center">
    <img src="https://grc.iit.edu/assets/images/logo-81e1c5c91f2ce84c3ea68ed772a4ef8c.png" width="300">
</p>

# WisIO: Workflow I/O Analysis Tool

![Build and Test](https://github.com/izzet/wisio/actions/workflows/ci.yml/badge.svg)
![PyPI - Version](https://img.shields.io/pypi/v/wisio?label=PyPI)
![PyPI - Wheel](https://img.shields.io/pypi/wheel/wisio?label=Wheel)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/wisio?label=Python)

## Overview

WisIO is an open-source tool designed to efficiently analyze multi-terabyte-scale workflow performance data over distributed resources. It provides a comprehensive analysis of I/O performance, identifying bottlenecks and potential root causes through advanced rule-based analysis. With its extensible design, WisIO can be tailored to various use cases, providing actionable insights for improving application performance and resource utilization. By leveraging parallel computing and multi-perspective views, WisIO enables rapid detection of complex I/O issues, making it an invaluable asset for HPC professionals and researchers.

## Installation

To install WisIO through `pip`, you will need to use the following command.

```bash
spack -e tools install
pip install wisio[darshan,dftracer]
```

To install WisIO from source, you will need to first install the dependencies:

```bash
spack -e tools install
pip install .[darshan,dftracer]
```

## Usage

```bash
wisio analysis=recorder analysis.trace_path=/path/to/recorder/traces
```
