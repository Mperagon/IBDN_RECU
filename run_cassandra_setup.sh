#!/bin/bash
export JAVA_HOME=$(ls -d ~/.sdkman/candidates/java/11* | head -1)
export PATH=$JAVA_HOME/bin:$PATH

# Install six for cqlsh
python3 -m pip install six --quiet 2>/dev/null || pip3 install six --quiet 2>/dev/null

# Run the CQL setup
~/cassandra/bin/cqlsh -f ~/practica_creativa/setup_cassandra.cql
echo "EXIT CODE: $?"

# Verify
~/cassandra/bin/cqlsh --execute "DESCRIBE KEYSPACE flight_data;"
