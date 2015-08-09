#!/bin/sh
cd ../..
tar czfv instagram-proto.tar.gz instagram-proto/

gcloud compute copy-files ./instagram-proto.tar.gz azurn@cassandra-5va8:~/
gcloud compute ssh azurn@cassandra-5va8