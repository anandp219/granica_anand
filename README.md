# Granica

Requirements
1. python >= 3.11
2. aws cli configured
3. terraform installed
4. kubectl installed

Steps
1. terraform -chdir=scripts init
2. terraform -chdir=scripts apply
3. aws eks --region ap-south-1 update-kubeconfig --name cluster-1 
4. kubectl apply -f scripts/access.yaml
5. rm -rf venv 
6. python -m venv venv
7. source venv/bin/activate
8. pip install -r requirements.txt 
9. export DAGSTER_HOME=~/dagster_home_test
10. dagit -f pipeline.py & # launches the ui
11. dagster job execute -f pipeline.py


After 
1. terraform -chdir=scripts destroy
