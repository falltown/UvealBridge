# go-store

## AKS cluster

Cluster: `uvealbridge` in resource group `uvealbridge` (centralindia).

```powershell
az aks start --name uvealbridge --resource-group uvealbridge
az aks stop  --name uvealbridge --resource-group uvealbridge
az aks show  --name uvealbridge --resource-group uvealbridge --query "powerState"
az aks get-credentials --name uvealbridge --resource-group uvealbridge --overwrite-existing
```

## Build & push image

ACR: `acruvealbridge.azurecr.io`. Bump the tag in [k8s/statefulset.yaml](k8s/statefulset.yaml) to match.

```powershell
$TAG = "0.8.0"

# Option A: build remotely in ACR (no local Docker needed)
az acr build --registry acruvealbridge --image go-store:$TAG .

# Option B: build locally and push
az acr login --name acruvealbridge
docker build -t acruvealbridge.azurecr.io/go-store:$TAG .
docker push acruvealbridge.azurecr.io/go-store:$TAG
```

## Deploy to AKS

```powershell
kubectl apply -f k8s/service-headless.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/statefulset.yaml

kubectl -n go-store rollout status statefulset/go-store
kubectl -n go-store get pods,svc

# Force a new image without changing the tag
kubectl -n go-store rollout restart statefulset/go-store
```
