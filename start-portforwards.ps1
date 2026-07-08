# Inicia todos los port-forwards de Kubernetes en background.
# Ejecutar desde PowerShell de Windows (no WSL2):
#   .\start-portforwards.ps1
# Para parar todo: Stop-Job * ; Remove-Job *

$forwards = @(
    @{ svc = "flask";               port = "5001:5001";   ns = "flight-prediction" },
    @{ svc = "airflow-webserver";   port = "8080:8080";   ns = "flight-prediction" },
    @{ svc = "mlflow";              port = "5000:5000";   ns = "flight-prediction" },
    @{ svc = "kibana";              port = "5601:5601";   ns = "flight-prediction" },
    @{ svc = "flink-jobmanager";    port = "8081:8081";   ns = "flight-prediction" },
    @{ svc = "spark-history-server";port = "18080:18080"; ns = "flight-prediction" },
    @{ svc = "nifi";                port = "8850:8080";   ns = "flight-prediction" },
    @{ svc = "minio";               port = "9001:9001";   ns = "flight-prediction" }
)

foreach ($f in $forwards) {
    Start-Job -Name $f.svc -ScriptBlock {
        param($svc, $port, $ns)
        kubectl port-forward "svc/$svc" $port -n $ns
    } -ArgumentList $f.svc, $f.port, $f.ns | Out-Null
    Write-Host "OK  $($f.svc)  →  localhost:$($f.port.Split(':')[0])"
}

Write-Host ""
Write-Host "Servicios accesibles:"
Write-Host "  Flask (predicciones)  →  http://localhost:5001/flights/delays/predict_kafka"
Write-Host "  Airflow               →  http://localhost:8080   (admin / admin)"
Write-Host "  MLflow                →  http://localhost:5000"
Write-Host "  Kibana                →  http://localhost:5601"
Write-Host "  Flink UI              →  http://localhost:8081"
Write-Host "  Spark History         →  http://localhost:18080"
Write-Host "  NiFi                  →  http://localhost:8850/nifi"
Write-Host "  MinIO Console         →  http://localhost:9001   (minioadmin / minioadmin)"
Write-Host ""
Write-Host "Para parar todo:  Stop-Job * ; Remove-Job *"
