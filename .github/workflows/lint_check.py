"""Module to provide linting for code before
"""

import sys
from pylint import lint

THRESHOLD = 9

#EXCLUSIONS = [
#    "too-many-arguments",           # Unecessarily restrictive
#    "too-many-branches",            # Unecessarily restrictive
#    "too-many-locals",              # Unecessarily restrictive
#    "too-many-statements",          # Unecessarily restrictive
#    "too-many-instance-attributes", # Unecessarily restrictive
#    "redefined-outer-name",         # Valid use cases for this with refresh of constant running code
#    "no-member",                    # Purposeful use case with ENUM
#    "protected-access",             # Unecessarily restrictive. Use case denote internal function
#    "dangerous-default-value",      # Valid use cases for this with setting empty arrays
#    "too-few-public-methods"        # Purposeful use case with ENUM
#]

# Disabled checks can be added by adding f"--disable={(','.join(EXCLUSIONS))}",
args = [
    "--max-line-length=150",
    "--max-module-lines=1500",
    "--output-format=json",
    sys.argv[1]
]

if len(sys.argv) < 2:
    raise FileNotFoundError ("File(s) to evaluate needs to be the first argument")

run = lint.Run(args, do_exit=False)

fatal_found = run.linter.stats.fatal
error_found = run.linter.stats.error
score = run.linter.stats.global_note

if  (fatal_found > 0) or (error_found > 0):
    print(f'Failed linting due to error/fatal issue. Fatal = {fatal_found}. Error = {error_found}')
    print('Please reviewing finding output above for information on issue(s) found.')
    sys.exit(1)
elif score < THRESHOLD:
    print(f'Failed linting due to score of {score}. Score of {THRESHOLD} needed to pass.')
    sys.exit(1)
else:
    print(f'Passed linting check with a score of {score}')
    sys.exit(0)
