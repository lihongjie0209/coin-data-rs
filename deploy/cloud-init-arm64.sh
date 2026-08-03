#!/bin/bash
set -euxo pipefail

RELEASE_URL="https://github.com/lihongjie0209/coin-data-rs/releases/download/v0.2.0/coin-data-rs-aarch64-unknown-linux-gnu.tar.gz"
RELEASE_SHA256="9d75e04defea299d1d577c1e2e1769872d049ec5ddf85549e858f25517fe15a3"

sed -i -E 's/^#?Port .*/Port 2222/' /etc/ssh/sshd_config
systemctl restart sshd

if ! id coin-data >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/coin-data-rs --shell /sbin/nologin coin-data
fi
install -d -o coin-data -g coin-data /var/lib/coin-data-rs/parquet
chown coin-data:coin-data /var/lib/coin-data-rs
install -d /usr/local/lib/coin-data-rs

curl --fail --location --retry 5 --output /tmp/coin-data-rs.tar.gz "$RELEASE_URL"
echo "$RELEASE_SHA256  /tmp/coin-data-rs.tar.gz" | sha256sum --check --strict
tar -xzf /tmp/coin-data-rs.tar.gz -C /tmp
install -m 0755 /tmp/coin-data-rs /usr/local/bin/coin-data-rs
install -m 0755 /tmp/libduckdb.so /usr/local/lib/coin-data-rs/libduckdb.so

cat >/etc/systemd/system/coin-data-rs.service <<'EOF'
[Unit]
Description=Binance market data collector (Rust)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=coin-data
Group=coin-data
EnvironmentFile=-/etc/coin-data-rs.env
Environment=RUST_LOG=coin_data=info
Environment=LD_LIBRARY_PATH=/usr/local/lib/coin-data-rs
ExecStart=/usr/local/bin/coin-data-rs --database /var/lib/coin-data-rs/market.duckdb --api-address 127.0.0.1:8081 --s3-prefix parquet/rust
Restart=always
RestartSec=5
TimeoutStopSec=30
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/coin-data-rs
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

if [ ! -f /swapfile ]; then
  dd if=/dev/zero of=/swapfile bs=1M count=2048 status=progress
  chmod 600 /swapfile
  mkswap /swapfile
  echo '/swapfile none swap defaults 0 0' >>/etc/fstab
fi
swapon -a

systemctl daemon-reload
systemctl enable --now coin-data-rs
rm -f /tmp/coin-data-rs.tar.gz /tmp/coin-data-rs /tmp/libduckdb.so
