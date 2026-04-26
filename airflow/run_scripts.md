To build the image:
Change the version of the image.
```bash
cd ..
docker build -t songchenxue/airflow:v1.1 -f airflow/Dockerfile .
docker push songchenxue/airflow:v1.1
```
Also, you need to apply all the rbac yaml files in the rbac directory:
```bash
kubectl apply -f rbac/airflow-sa.yaml
kubectl apply -f rbac/airflow-job-trigger.yaml
kubectl apply -f rbac/airflow-secret.yaml
kubctl apply -f rbac/airflow-secrtes.yaml
```

After that, upgrade the airflow helm release:
```bash
helm upgrade airflow apache-airflow/airflow -n airflow -f airflow/values.yaml
```

apply nodeport yaml file:
```bash
kubectl apply -f airflow/airflow-api-nodeport.yaml
```

if the dag is not updated, you can reserialize 
```bash
kubectl exec -it airflow-scheduler-0 -n airflow -- airflow dags reserialize
```

if you want to delete the k8s image:
```bash
sudo crictl images | grep airflow
sudo crictl rmi <image_id>
```


### First install command
``` bash
cd /path/to/Recommendation-System-for-Jellyfin
# install helm
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
helm version

helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm install airflow apache-airflow/airflow \
  -n airflow \
  --create-namespace

kubectl apply -f rbac/ -R

helm upgrade airflow apache-airflow/airflow \
  -n airflow \
  -f airflow/values.yaml
```
