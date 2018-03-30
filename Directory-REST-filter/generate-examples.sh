#!/bin/bash

DESTDIR="BioSchemas-examples"

rm -rf $DESTDIR
mkdir $DESTDIR
for i in bbmri-eric:ID:CZ_MMCI:collection:LTS bbmri-eric:ID:CZ_MMCI; do ./get-bioschemas.pl $i >${DESTDIR}/${i//:/-}_jsonld.json; done
