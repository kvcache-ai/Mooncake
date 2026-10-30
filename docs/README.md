# Mooncake documents

## Build the docs

- Make sure in `docs` directory

```bash
cd docs
```

- Install the dependencies:

```bash
pip install -r requirements-docs.txt
```

- Clean the previous build (optional but recommended):

```bash
make clean
```

- Generate the HTML documentation:

```bash
make html
```

Be sure to set `locale` right before building.

## Open the docs with your browser

- Serve the documentation locally:

```bash
python -m http.server -d build/html/
```

This will start a local server at http://localhost:8000. You can now open your browser and view the documentation.

If port 8000 is already in use, you can specify a different port, for example:

```bash
python -m http.server 3000 -d build/html/
```

## PyPI downloads page

`source/community/pypi-downloads.md` uses a same-origin snapshot in
`source/_static/pypi-downloads.json`; it does not depend on an external app.
The Pages workflow refreshes this snapshot during deployment and runs daily.
Successful endpoint responses are reused for 24 hours, and the workflow cache
keeps the last available responses when PyPIStats is unavailable. Collection
age and partial data are visible on the page. The committed snapshot also
supports offline documentation builds.

To refresh manually and run the offline checks, from `docs/`:

```bash
python scripts/update_pypi_downloads.py
python -m unittest discover -s tests -p 'test_pypi_downloads.py'
node --test tests/pypi-downloads.test.cjs
```

Avoid repeatedly fetching the same PyPIStats endpoints within a day.
