cprofile:
	python -m cProfile -o data/current.cprofile cli.py newrun 2>data/stderr.log
	echo '		Snakevix URL: https://$(C9_HOSTNAME)/snakeviz/%2Fhome%2Fubuntu%2Fworkspace%2Fcurrent.cprofile'
	snakeviz -s -H$(C9_IP) data/current.cprofile
