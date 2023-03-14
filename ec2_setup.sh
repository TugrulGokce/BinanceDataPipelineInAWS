#!/bin/bash
cd /home/ec2-user || exit
sudo yum update -y
sudo yum install htop -y
sudo amazon-linux-extras install epel -y
sudo amazon-linux-extras install python3.8 -y
sudo yum install git -y
cd /home/ec2-user || exit
git clone https://github.com/sinanartun/binance_4.git
sudo chown -R ec2-user:ec2-user /home/ec2-user/binance_4
sudo chmod 2775 /home/ec2-user/binance_4 && find /home/ec2-user/binance_4 -type d -exec sudo chmod 2775 {} \;
cd binance_4 || exit
python3.8 -m venv venv
sudo sed -i 's/^PATH=.*/&:\/usr\/local\/bin/' /root/.bash_profile
sed -i 's/^PATH=.*/&:\/usr\/local\/bin/' /home/ec2-user/.bash_profile
source ~/.bash_profile
pip3.8 install -r requirements.txt
sudo tee /etc/systemd/system/binance.service << EOF
[Unit]
Description=Binance Service

[Service]
User=ec2-user
ExecStart=/bin/python3.8 /home/ec2-user/binance_4/main.py
Restart=always

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable binance
sudo systemctl start binance