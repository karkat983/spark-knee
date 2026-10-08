# Setup

## Requirements

| Component | Needed | Verified with |
|-----------|--------|---------------|
| Java | JDK 17 or 21 (Spark 4 does not run on Java 8/11) | OpenJDK 17.0.20.1 (Homebrew) |
| Python | 3.10-3.13 for PySpark 4.x | Python 3.12.15 |
| PySpark | >= 3.5 (4.x recommended) | 4.2.0 |
| Disk | ~1 GB per 16x of sized data | |

## Java

PySpark starts a JVM, so a JDK must be installed and `JAVA_HOME` set:

```bash
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

Without it, `SparkSession.builder.getOrCreate()` fails with
`JAVA_GATEWAY_EXITED` / "Unable to locate a Java Runtime".

## Python version must match on driver and workers

Spark launches separate Python worker processes for Python code such as `createDataFrame` on
local lists or UDFs. If the workers start a different Python minor version than the driver,
tasks fail with `PYTHON_VERSION_MISMATCH`. `src/spark_session.py` and the test fixture set
`PYSPARK_PYTHON` to the running interpreter to prevent this. If you run Spark another way, set:

```bash
export PYSPARK_PYTHON="$(which python)"
```

## Virtual environment

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest
```
