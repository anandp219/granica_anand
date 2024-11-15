# Granica

Requirements
1. python >= 3.11
2. aws cli configured
3. terraform installed
4. kubectl installed

Steps
1. terraform -chdir=scripts init
2. terraform -chdir=scripts apply
3. aws eks --region ap-south-1 update-kubeconfig --name cluster-2
4. kubectl apply -f scripts/access.yaml
5. rm -rf venv 
6. python -m venv venv
7. source venv/bin/activate
8. pip install -r requirements.txt 
9. rm -rf ~/dagster_home_test && mkdir ~/dagster_home_test
10. DAGSTER_HOME=~/dagster_home_test dagster-webserver -f pipeline.py &
11. DAGSTER_HOME=~/dagster_home_test dagster job execute -f pipeline.py


After 
1. terraform -chdir=scripts destroy


Usages
1. Dagster for pipeline orchestration
2. Terraform for infrastructure provisioning
3. AWS EKS for kubernetes cluster
4. Kubectl for kubernetes cluster management
5. Python for pipeline development
6. AWS CLI for aws management

Further work
1. Enable hive support
2. Enable query via client
3. Spark optimization
