# Activate virtual environment
venv\Scripts\activate

# Set Google credentials
$env:GOOGLE_APPLICATION_CREDENTIALS="E:\Job Search\Project Ideas\Ecommerce Price Engineer Analytics\amazon-price-analytics\gcp_key.json"
$env:GCP_PROJECT_ID="ecommerce-price-analytics"

Write-Host "✅ Environment ready!" -ForegroundColor Green
Write-Host "Project: $env:GCP_PROJECT_ID" -ForegroundColor Cyan
