#!/bin/bash
set -e

HOST="github.com"
OWNER="orca-strategy"
REPO="orca-strategy"

apt-get update -y
apt-get install -y git python3 python3-pip

rm -rf /root/orca-strategy
git clone "https://${HOST}/${OWNER}/${REPO}.git" /root/orca-strategy

cd /root/orca-strategy
pip3 install --break-system-packages -r requirements.txt 2>/dev/null || pip3 install -r requirements.txt

touch /root/orca-strategy/.env
chmod 600 /root/orca-strategy/.env

cp /root/orca-strategy/crypto-bot.service /etc/systemd/system/crypto-bot.service
chmod +x /root/orca-strategy/set_api.sh

systemctl daemon-reload
bash /root/orca-strategy/set_api.sh
