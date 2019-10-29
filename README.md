
# logzip

An efficient compression tool specific for log files.  It compresses log files by utilizing the inherent structures of raw log messages, and thereby achieves a high compression ratio.  

## Prerequisites 

- python3
- pandas

## Installation 

Logzip can be directly execute through source code. 

1. Download and install python3 [here](https://www.python.org/downloads/).

2. Install Pandas.

   ```$ pip3 install pandas```

3. Clone logzip.

   ``` $ clone https://github.com/logpai/logzip.git``` 

## Data

We've conducted comprehensive experiments to evaluate the efficiency of logzip on five real-world datasets. All the datasets that we use are available at [loghub](https://github.com/logpai/loghub).

## Usage

A demo is uploaded to this repo (logzip/src/demo). We use a HDFS log file with 2k lines as a demo.

#### Compression

```shell
$ cd logzip/demo/
$ python3 zip_demo.py
```
