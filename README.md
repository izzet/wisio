<p align="center">
    <img src="./assets/logo.png">
</p>

# WisIO: Automated I/O Analysis for HPC Workflows

[![Build and Test](https://github.com/izzet/wisio/actions/workflows/ci.yml/badge.svg)](https://github.com/izzet/wisio/actions/workflows/ci.yml)

## Overview

**"Extract wisdom from large-scale rich I/O traces of HPC workflows."**

## Installation

To install WisIO through `pip`, you will need to use the following command.

```bash
spack -e tools install
pip install wisio[darshan]
```

To install WisIO from source, you will need to first install the dependencies:

```bash
spack -e tools install
pip install .[darshan]
```

## Usage

```bash
wisio analyze -c /path/to/config
```
