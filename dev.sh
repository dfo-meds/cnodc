
# Run test cases
python -m tests.run

# Output coverage report
python -m coverage html

# Run bandit for security issues
python -m bandit -r src -f txt -o report_bandit.txt

# Audit dependencies for issues
pip-audit
