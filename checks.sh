echo "running ruff fixes"
ruff check --fix --unsafe-fixes

echo "running mypy"
mypy --explicit-package-bases .

echo "running tests"
pytest tests/