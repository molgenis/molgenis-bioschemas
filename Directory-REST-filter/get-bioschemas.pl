#!/usr/bin/perl -w

# gets GeoJSON from the BBMRI-ERIC Directory REST API
#
# Usage:
# ./get-bioschemas.pl biobank_ID_or_collection_ID

use strict;
use utf8;
use Switch;

use Scalar::MoreUtils qw(empty);
use REST::Client;
use LWP::Simple;
use JSON;
use Time::HiRes qw(usleep);
use URI::Escape;
use Storable qw(dclone);
use Data::Validate::URI qw(is_uri);
use Locale::Country;

use Data::Dumper;

my $GeoCoordPattern = qr/^-?\d+\.\d+$/;
my $DirectoryHost="https://directory.bbmri-eric.eu/api/v2/";
my $DirectoryBiobanks=$DirectoryHost . "eu_bbmri_eric_biobanks";
my $DirectoryCollections=$DirectoryHost . "eu_bbmri_eric_collections";
my $DirectoryContacts=$DirectoryHost . "eu_bbmri_eric_persons";
my $DirectoryDiseaseTypes=$DirectoryHost . "eu_bbmri_eric_disease_types";

my $client = REST::Client->new();

die "Identifier of entity to generate BioSchema metadata for must be provided!" if (!@ARGV);
my @target_ids = @{ dclone(\@ARGV) };

my %biobanks;
my %contacts;
my %collections;
# lookup optimization
my %collections_to_biobanks_map;
my %all_entries_by_dn;
my %all_entries_by_ID;

my $REST_URL;
my $parsesub;

# get biobanks
$parsesub = <<'END_OF_CODE';
	$biobanks{$i->{'id'}}{"biobankID"} = $i->{'id'};
	$biobanks{$i->{'id'}}{"biobankName"} = $i->{'name'};
	$biobanks{$i->{'id'}}{"biobankAcronym"} = $i->{'acronym'};
	$biobanks{$i->{'id'}}{"biobankJuridicalPerson"} = $i->{'juridical_person'};
	$biobanks{$i->{'id'}}{"biobankDescription"} = $i->{'description'};
	$biobanks{$i->{'id'}}{"biobankType"} = "biobank";
	$biobanks{$i->{'id'}}{"geoLongitude"} = $i->{'longitude'} if defined $i->{'longitude'};
	$biobanks{$i->{'id'}}{"geoLatitude"} = $i->{'latitude'} if defined $i->{'latitude'};
	$biobanks{$i->{'id'}}{"contactIDRef"} = $i->{'contact'}{'id'} if defined $i->{'contact'} && defined $i->{'contact'}{'id'};
	die "Duplicate ID found: ", $i->{'id'}, "\n" if (defined $all_entries_by_ID{$i->{'id'}});
	$all_entries_by_ID{$i->{'id'}} = 'biobank';
	$biobanks{$i->{'id'}}{"collections"} = $i->{'collections'};
END_OF_CODE
;

&get_paginated_REST($DirectoryBiobanks, $parsesub);
#print Dumper(%biobanks);
#print Dumper(%all_entries_by_ID);
#exit;

# get collections
$parsesub = <<'END_OF_CODE';
	$collections{$i->{'id'}}{"collectionID"} = $i->{'id'};
	$collections{$i->{'id'}}{"collectionOrderOfMagnitude"} = $i->{'order_of_magnitude'}->{'id'} if defined $i->{'order_of_magnitude'};
	$collections{$i->{'id'}}{"diagnosis_available"} = $i->{'diagnosis_available'} if defined $i->{'diagnosis_available'};
	$collections{$i->{'id'}}{"materials"} = $i->{'materials'} if defined $i->{'materials'};
	$collections{$i->{'id'}}{"storage_temperature"} = $i->{'storage_temperature'} if defined $i->{'storage_temperature'};
	$collections{$i->{'id'}}{"data_categories"} = $i->{'data_categories'} if defined $i->{'data_categories'};
	my $b = $i->{'biobank'}->{'id'} || die "Orphaned collection: ", $i->{'id'}, "\n";
	$collections{$i->{'id'}}{"biobank"} = $b;
	push (@{$collections_to_biobanks_map{$b}}, $i->{'id'});
	die "Duplicate ID found: ", $i->{'id'}, "\n" if (defined $all_entries_by_ID{$i->{'id'}});
	$all_entries_by_ID{$i->{'id'}} = 'collection';
END_OF_CODE
;
&get_paginated_REST($DirectoryCollections, $parsesub);
#print Dumper(%collections);
#exit;

# get contacts
$parsesub = <<'END_OF_CODE';
	$contacts{$i->{'id'}}{"contactEmail"} = $i->{'email'} if defined $i->{'email'};
	$contacts{$i->{'id'}}{"contactAddress"} = $i->{'address'} if defined $i->{'address'};
	$contacts{$i->{'id'}}{"contactCity"} = $i->{'city'} if defined $i->{'city'};
	$contacts{$i->{'id'}}{"contactCountry"} = $i->{'country'}->{'id'} if defined $i->{'country'}->{'id'};
END_OF_CODE
;
&get_paginated_REST($DirectoryContacts, $parsesub);
#print Dumper(%contacts);
#exit;

my %JSON_biobank_collections;
my @BSch_JSON_collections;

my %onto_mapping_materials = (
	"WHOLE_BLOOD" => {
		"url" => "http://purl.obolibrary.org/obo/OBI_0000655",
		"name" => "blood specimen",
	},
	"FAECES" => {
		"url" => "http://purl.obolibrary.org/obo/OBI_0002503",
		"name" => "feces specimen",
	},
	"PLASMA" => {
		"url" => "http://purl.obolibrary.org/obo/OBI_0100016",
		"name" => "blood plasma specimen",
	},
	"SALIVA" => {
		"url" => "http://purl.obolibrary.org/obo/OBI_0002507",
		"name" => "saliva specimen",
	},
	"SERUM" => {
		"url" => "http://purl.obolibrary.org/obo/OBI_0100017",
		"name" => "blood serum specimen",
	},
	"TISSUE_FROZEN" => {
		"url" => "http://purl.obolibrary.org/obo/OBI_0000922",
		"name" => "frozen specimen",
	},
);

my %onto_mapping_data_avail = (
	"SURVEY_DATA" => {
		"url" => "http://purl.obolibrary.org/obo/OMIABIS_0000060",
		"name" => "survey data",
	},
	"MEDICAL_RECORDS" => {
		"url" => "http://purl.obolibrary.org/obo/OMIABIS_0001027",
		"name" => "sample medical record",
	},
);

my %diag_details;


my $JSON_encoder = JSON->new;
$JSON_encoder->pretty(1);
$JSON_encoder->canonical(1);

foreach my $target_id (@target_ids) {
	my $JSON_text;
	my %BSch_JSON_entity;

	if (!defined $all_entries_by_ID{$target_id}) {
		die "Undefined ID\n";
	}

	elsif ($all_entries_by_ID{$target_id} eq "biobank") {
		$BSch_JSON_entity{'@context'} = "http://schema.org";
		$BSch_JSON_entity{'@type'} = "Organization";
		$BSch_JSON_entity{'url'} = "https://directory.bbmri-eric.eu/menu/main/dataexplorer/details/eu_bbmri_eric_biobanks/" . $biobanks{$target_id}{"biobankID"};
		$BSch_JSON_entity{'identifier'} = $biobanks{$target_id}{"biobankID"};
		$BSch_JSON_entity{'name'} = $biobanks{$target_id}{"biobankName"};
		$BSch_JSON_entity{'alternateName'} = $biobanks{$target_id}{"biobankAcronym"};
		$BSch_JSON_entity{'legalName'} = $biobanks{$target_id}{"biobankJuridicalPerson"};
		$BSch_JSON_entity{'description'} = $biobanks{$target_id}{"biobankDescription"};
		my $contactID = $biobanks{$target_id}{"contactIDRef"};
		$BSch_JSON_entity{'email'} = $contacts{$contactID}{"contactEmail"};
		$BSch_JSON_entity{'address'}{'@type'} = "PostalAddress";
		$BSch_JSON_entity{'address'}{'contactType'} = "juridical person";
		$BSch_JSON_entity{'address'}{'streetAddress'} = $contacts{$contactID}{"contactAddress"}  if defined $contacts{$contactID}{"contactAddress"};
		my @locality_elements;
		push @locality_elements, $contacts{$contactID}{"contactCity"} if defined $contacts{$contactID}{"contactCity"};
		push @locality_elements, code2country(lc($contacts{$contactID}{"contactCountry"})) if defined $contacts{$contactID}{"contactCountry"};
		$BSch_JSON_entity{'address'}{'addressLocality'} = (@locality_elements > 0 ? join(", ", @locality_elements) : "");
	}

	elsif ($all_entries_by_ID{$target_id} eq "collection") {
		$BSch_JSON_entity{'@context'} = 'http://schema.org';
		$BSch_JSON_entity{'@type'} = [ "DataRecord" ];
		$BSch_JSON_entity{'@id'} = $biobanks{$target_id}{"collectionID"};
		$BSch_JSON_entity{'provider'}{'@type'} = "Organization";
		$BSch_JSON_entity{'provider'}{'identifier'} = $collections{$target_id}{"biobank"};
		$BSch_JSON_entity{'provider'}{'@id'} = "https://directory.bbmri-eric.eu/menu/main/dataexplorer/details/eu_bbmri_eric_biobanks/" . $collections{$target_id}{"biobank"};
		$BSch_JSON_entity{'provider'}{'url'} = "https://directory.bbmri-eric.eu/menu/main/dataexplorer/details/eu_bbmri_eric_biobanks/" . $collections{$target_id}{"biobank"};
		my @BSch_JSON_entity_add_prop;
		foreach my $prop ("diagnosis_available", "materials", "storage_temperatures", "data_categories", "standards") {
			foreach my $prop_element_ref (@{$collections{$target_id}{$prop}}) {
				my %prop_element = %$prop_element_ref;
				my %BSch_JSON_prop = ( 'name' => $prop );
				switch ($prop) {
					case "diagnosis_available" {
						$BSch_JSON_prop{"value"} = $prop_element{"id"};
						$BSch_JSON_prop{"url"} = "http://purl.obolibrary.org/obo/OGMS_0000073";
						$BSch_JSON_prop{"valueReference"}[0]{'@type'} = "CategoryCode";
						if (!defined $diag_details{$prop_element{"id"}}{"label"}) {
							my $json_output;
							my $REST_URL = $DirectoryDiseaseTypes . "/" . $prop_element{"id"};
							print STDERR "Resolving disease code $REST_URL\n";
							$client->GET($REST_URL);
							$json_output = decode_json($client->responseContent());
							$diag_details{$json_output->{'id'}}{"label"} = $json_output->{'label'};
							$diag_details{$json_output->{'id'}}{"code"} = $json_output->{'code'};
						}
						$BSch_JSON_prop{"valueReference"}[0]{'name'} = $diag_details{$prop_element{"id"}}{"label"} if defined $diag_details{$prop_element{"id"}}{"label"};
						$BSch_JSON_prop{"valueReference"}[0]{'url'} = "https://directory.bbmri-eric.eu/api/v2/eu_bbmri_eric_disease_types/" . $prop_element{"id"};
						$BSch_JSON_prop{"valueReference"}[0]{'codeValue'} = $diag_details{$prop_element{"id"}}{"code"} if defined $diag_details{$prop_element{"id"}}{"code"};
					}
					case "materials" {
						$BSch_JSON_prop{"value"} = $prop_element{"label"};
						$BSch_JSON_prop{"valueReference"}[0]{'@type'} = "CategoryCode";
						$BSch_JSON_prop{"valueReference"}[0]{'name'} = $prop_element{"label"}; 
						$BSch_JSON_prop{"valueReference"}[0]{'url'} = "https://directory.bbmri-eric.eu/api/v2/eu_bbmri_eric_material_types/" . $prop_element{"id"};
						$BSch_JSON_prop{"valueReference"}[0]{'codeValue'} = $prop_element{"id"};
						if (defined $onto_mapping_materials{$prop_element{"id"}}) {
							$BSch_JSON_prop{"valueReference"}[1]{'@type'} = "CategoryCode";
							$BSch_JSON_prop{"valueReference"}[1]{'name'} = $onto_mapping_materials{$prop_element{"id"}}{"name"};
							$BSch_JSON_prop{"valueReference"}[1]{'url'} = $onto_mapping_materials{$prop_element{"id"}}{"url"};
						}
					}
					case "storage_temperatures" {
						$BSch_JSON_prop{"value"} = $prop_element{"id"};
						$BSch_JSON_prop{"url"} = "http://purl.obolibrary.org/obo/OMIABIS_0001013";
					}
					case "data_categories" {
						$BSch_JSON_prop{"value"} = $prop_element{"id"};
						if (defined $onto_mapping_data_avail{$prop_element{"id"}}) {
							$BSch_JSON_prop{"valueReference"}[0]{'@type'} = "CategoryCode";
							$BSch_JSON_prop{"valueReference"}[0]{'name'} = $onto_mapping_data_avail{$prop_element{"id"}}{"name"};
							$BSch_JSON_prop{"valueReference"}[0]{'url'} = $onto_mapping_data_avail{$prop_element{"id"}}{"url"};
						}
					}
				}
				push @BSch_JSON_entity_add_prop, \%BSch_JSON_prop;
			}
		}
		$BSch_JSON_entity{'additionalProperty'} = \@BSch_JSON_entity_add_prop;
	}

	else {
		die "Unsupported type of ID (but still defined) - ", $all_entries_by_ID{$target_id}, "\n";
	}

	$JSON_text = $JSON_encoder->encode(\%BSch_JSON_entity);
	print "$JSON_text";

	# This is Data Catalog and Data Record glue, to support mapping from biobanks to their collections
	if ($all_entries_by_ID{$target_id} eq "biobank") {
		undef %BSch_JSON_entity;
		$BSch_JSON_entity{'@context'} = "http://schema.org";
		$BSch_JSON_entity{'@type'} = "DataCatalog";
		$BSch_JSON_entity{'provider'}{'@type'} = "Organization";
		$BSch_JSON_entity{'provider'}{'identifier'} = $biobanks{$target_id}{"biobankID"};
		$BSch_JSON_entity{'provider'}{'@id'} = "https://directory.bbmri-eric.eu/menu/main/dataexplorer/details/eu_bbmri_eric_biobanks/" . $biobanks{$target_id}{"biobankID"};
		$BSch_JSON_entity{'provider'}{'url'} = "https://directory.bbmri-eric.eu/menu/main/dataexplorer/details/eu_bbmri_eric_biobanks/" . $biobanks{$target_id}{"biobankID"};
		my @collections = @{$biobanks{$target_id}{"collections"}};
		for (my $dataSetId = 0; $dataSetId < @collections; $dataSetId++) {
			$BSch_JSON_entity{'dataset'}[$dataSetId]{'@type'} = 'DataRecord';
			$BSch_JSON_entity{'dataset'}[$dataSetId]{'@id'} = $collections[$dataSetId]->{"_href"};
			$BSch_JSON_entity{'dataset'}[$dataSetId]{'url'} = $collections[$dataSetId]->{"_href"};
			$BSch_JSON_entity{'dataset'}[$dataSetId]{'identifier'} = $collections[$dataSetId]->{"id"};
			$BSch_JSON_entity{'dataset'}[$dataSetId]{'name'} = $collections[$dataSetId]->{"name"};
		}
		$JSON_text = $JSON_encoder->encode(\%BSch_JSON_entity);
		print "\n\n$JSON_text";
	}

}



sub get_paginated_REST($&) {
	my $REST_URL = shift;
	my $code = shift;

	do {
		my $json_output;
		print STDERR "Getting $REST_URL\n";
		$client->GET($REST_URL);
		$json_output = decode_json($client->responseContent());

		#print Dumper($json_output);
		#exit;

		foreach my $i (@{$json_output->{'items'}}) {
			eval $code;
			die $@ if $@;
		}

		if (not empty($json_output->{'nextHref'})) {
			$REST_URL = $json_output->{'nextHref'};
			die "Unknown URL format:\n$REST_URL\n" if (!is_uri($REST_URL));
		}
		else {
			$REST_URL = "";
		}

	} while ($REST_URL ne "");
}
