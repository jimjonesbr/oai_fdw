#!/bin/bash

BASEPATH=$(pwd)
cd $BASEPATH/postgresql-14; ./deploy-postgres-14.sh
cd $BASEPATH/postgresql-13; ./deploy-postgres-13.sh
cd $BASEPATH/postgresql-12; ./deploy-postgres-12.sh
cd $BASEPATH/postgresql-11; ./deploy-postgres-11.sh

#postgresql-13/deploy-postgres-13.sh
#postgresql-12/deploy-postgres-12.sh
#postgresql-11/deploy-postgres-11.sh

