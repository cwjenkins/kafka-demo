#!/bin/bash

echo "Rebuild apps"
mvn clean compile

echo "Starting transformer..."
mvn exec:java@transformer &
echo "Transformer started!"

echo "Starting producer..."
mvn exec:java@producer
echo "Exiting..."
