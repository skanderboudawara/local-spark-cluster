echo "running ruff fixes"
ruff check --fix --unsafe-fixes

echo "running tests"
pytest tests/