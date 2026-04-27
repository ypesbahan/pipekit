# pipekit

A lightweight Python library for defining and running local ETL pipelines with minimal config.

---

## Installation

```bash
pip install pipekit
```

---

## Usage

Define your pipeline steps as simple functions and wire them together with `Pipeline`.

```python
from pipekit import Pipeline, step

@step
def extract():
    return [{"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}]

@step
def transform(data):
    return [{"id": row["id"], "value": row["value"].upper()} for row in data]

@step
def load(data):
    for row in data:
        print(f"Loading: {row}")

pipeline = Pipeline([extract, transform, load])
pipeline.run()
```

**Output:**
```
Loading: {'id': 1, 'value': 'FOO'}
Loading: {'id': 2, 'value': 'BAR'}
```

---

## Features

- Minimal boilerplate — define steps as plain Python functions
- Linear and branching pipeline support
- Built-in logging and basic error handling
- No external dependencies

---

## Requirements

- Python 3.8+

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.