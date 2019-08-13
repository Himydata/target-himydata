# target-himydata

Reads [Singer](https://singer.io) formatted data from stdin and persists it to the [Himydata Platform Import API](https://www.himydata.com/).

## Install

Requires Python 3

```bash
› pip install target-himydata
```

or 

```bash
› python setup.py install
```

## Use

target-himydata takes two types of input:

1. A config file containing your Himydata client id and api key
2. A stream of Singer-formatted data on stdin

Create config file to contain your Himydata client id and api key:

```json
{
  "api_key" : "99999999999999999999999999",
  "himydata_url": "https://platform.himydata.com"
}
```

```bash
› tap-some-api | target-himydata --config config.json
› tap-csv --config  config-csv.json | target-himydata --config config-himydata.json
```

where `tap-some-api` is [Singer Tap](https://singer.io).

---

Copyright &copy; 2018 Himydata