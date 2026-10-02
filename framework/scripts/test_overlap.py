"""List tests whose covered lines are identical, from a `pytest --cov-context=test` coverage file.

Usage: python scripts/test_overlap.py <coverage file>
"""

import collections
import sys

from coverage import CoverageData

data = CoverageData(sys.argv[1])
data.read()

lines_by_test: dict[str, set[tuple[str, int]]] = collections.defaultdict(set)
for path in data.measured_files():
    for lineno, contexts in data.contexts_by_lineno(path).items():
        for context in contexts:
            if context:  # "" is import-time code, outside any test
                lines_by_test[context.rsplit("|", 1)[0]].add((path, lineno))

hits = collections.Counter(line for lines in lines_by_test.values() for line in lines)
redundant = sum(all(hits[line] > 1 for line in lines) for lines in lines_by_test.values())
print(f"{len(lines_by_test)} tests, {redundant} add no line that another test misses")

groups: dict[frozenset, list[str]] = collections.defaultdict(list)
for test, lines in lines_by_test.items():
    groups[frozenset(lines)].append(test)

for lines, tests in sorted(groups.items(), key=lambda group: -len(group[1])):
    if len(tests) > 1:
        print(f"\n{len(tests)} tests hit the same {len(lines)} lines:")
        print("  " + "\n  ".join(sorted(tests)))
