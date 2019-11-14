tags:
	rm -f TAGS
	find . -type f -name '*.py' -print0 | xargs -0 etags --append


find_caps:
	find . -name '*.py' -print0 | xargs -0 egrep '[^A-Za-z][A-Z]([A-Z_]+)[^a-zA-Z]' | grep -v 'C[.]' | grep -v /ctools | grep -v /dfxml | grep -v /constants.py
