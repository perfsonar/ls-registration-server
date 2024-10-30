#!/bin/bash

#NOTE: On mac, the sed -i option does not work without a backup extension.

#Create ./elastic_data directory if doesn't exist
echo "[Data directory setup]"
if [ ! -d "./elastic_data" ]; then
    echo "Creating ./elastic_data directory"
  mkdir elastic_data
else
    echo "./elastic_data directory already exists"
fi
echo ""

#Copy env.example to .env if .env doesn't exist
echo "[Environment file setup]"
if [ ! -f ".env" ]; then
    echo "Copying env.example to .env"
  cp env.example .env
else
    echo ".env file already exists"
fi
echo ""

#If ELASTIC_PASSWORD=CHANGE in .env, generate a random password and update .env
echo "[Elasticsearch password setup]"
if grep -q "ELASTIC_PASSWORD=CHANGE" .env; then
    echo "Generating random password for Elasticsearch"
    ELASTIC_PASSWORD=$(openssl rand -base64 32)
    sed -i "s/ELASTIC_PASSWORD=.*/ELASTIC_PASSWORD=${ELASTIC_PASSWORD}/g" .env
    echo "Elasticsearch password updated in .env"
else
    echo "Elasticsearch password already set in .env"
fi
echo ""

#If KIBANA_PASSWORD=CHANGE in .env, generate a random password and update .env
echo "[Kibana password setup]"
if grep -q "KIBANA_PASSWORD=CHANGE" .env; then
    echo "Generating random password for Kibana"
    KIBANA_PASSWORD=$(openssl rand -base64 32)
    sed -i "s/KIBANA_PASSWORD=.*/KIBANA_PASSWORD=${KIBANA_PASSWORD}/g" .env
    echo "Kibana password updated in .env"
else
    echo "Kibana password already set in .env"
fi
echo ""

#If KIBANA_ENCRYPTION_KEY=CHANGE in .env, generate a random 32 character string and update .env
echo "[Kibana encryption key setup]"
if grep -q "KIBANA_ENCRYPTION_KEY=CHANGE" .env; then
    echo "Generating random encryption key for Kibana"
    KIBANA_ENCRYPTION_KEY=$(openssl rand -base64 32)
    sed -i "s/KIBANA_ENCRYPTION_KEY=.*/KIBANA_ENCRYPTION_KEY=${KIBANA_ENCRYPTION_KEY}/g" .env
    echo "Kibana encryption key updated in .env"
else
    echo "Kibana encryption key already set in .env"
fi
echo ""

# Done  
echo "[Setup complete]"
echo "Check .env for any password or key changes."
echo ""

