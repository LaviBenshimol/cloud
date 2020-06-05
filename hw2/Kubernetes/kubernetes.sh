#create the resources
kubectl apply -f hw2_resources.yaml

sleep 3

sevIPaddr=$(kubectl get service/hw2v1 -o jsonpath='{.spec.clusterIP}')
end=$((SECONDS+240))
while [ ${SECONDS} -lt ${end} ]; do
wget $sevIPaddr -q -O- ;
done

#update to version 2
kubectl apply -f deploy_update.yaml

sleep 5
sevIPaddr=$(kubectl get service/hw2v1 -o jsonpath='{.spec.clusterIP}')
wget $sevIPaddr -q -O- ;

#rollback to version 1
kubectl rollout undo deployment.apps/hw2v1 --to-revision=1

sleep 8
sevIPaddr=$(kubectl get service/hw2v1 -o jsonpath='{.spec.clusterIP}')
wget $sevIPaddr -q -O- ;

#delete the deployment, service, and aoutoscaler
kubectl delete service/hw2v1  deployment.apps/hw2v1 horizontalpodautoscaler.autoscaling/hw2v1

echo Great Success