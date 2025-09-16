# side-project-quant-pipeline


Local installs on macOS
You only need these on your Mac:

Docker Desktop → runs all containers.

VS Code → your IDE.

VS Code extensions:

Docker

Dev Containers

Python

YAML

Git → to manage your repo.

# Bring it up all services in docker: 
docker-compose up -d
**remarks: ensure Redpanada image to be: image: redpandadata/redpanda:v23.3.5 or latest 

# Rebuild  docker: 
docker-compose build --no-cache devcontainer
