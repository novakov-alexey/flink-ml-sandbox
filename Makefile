.ONESHELL:
.SHELL := /bin/bash

create-env:
	python3 -m venv dl-on-flink && source dl-on-flink/bin/activate

install:
	python3 -m pip install -r requirements.txt