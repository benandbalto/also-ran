.PHONY: test check ansible-check

ANSIBLE_PLAYBOOK ?= ansible-playbook

test:
	ALSO_RAN_DEPLOYMENT=$(CURDIR)/controller/also-ran/deployment.json PYTHONPATH=controller/also-ran python3 -m unittest discover -s controller/also-ran/tests -v

check: test
	python3 scripts/validate-config.py --deployment controller/also-ran/deployment.json --policy controller/also-ran/gcp-acceptance-policy.json

ansible-check:
	$(ANSIBLE_PLAYBOOK) --syntax-check -i examples/hosts.ini ansible/playbooks/also-ran.yml
	$(ANSIBLE_PLAYBOOK) --syntax-check -i examples/hosts.ini ansible/playbooks/runner-image-builder.yml
