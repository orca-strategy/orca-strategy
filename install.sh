GH="github.com"
REPO="orca-strategy"
apt-get install -y git python3 python3-pip
rm -rf /root/${REPO}
git clone "https://${GH}/${REPO}/${REPO}.git" /root/${REPO}
cd /root/${REPO}
pip3 install -r requirements.txt
touch /root/${REPO}/.env && chmod 600 /root/${REPO}/.env
cp /root/${REPO}/crypto-bot.service /etc/systemd/system/crypto-bot.service
chmod +x /root/${REPO}/set_api.sh
systemctl daemon-reload
bash /root/${REPO}/set_api.sh
