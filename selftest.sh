#!/bin/sh

cd node
cargo build
cargo run -- --bind=localhost:25188 --datadir=selftest_datadir1 &
sleep 1
cargo run -- --bind=localhost:25189 --datadir=selftest_datadir2 --addnode=localhost:25188 &
sleep 1

echo
echo ">>> Press any key to exit"
read

killall braidpool-node
rm -rf selftest_datadir1 selftest_datadir2
