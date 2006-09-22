#!/usr/bin/perl -w
use strict;
use Module::Build;

my $build = Module::Build->new(
    dist_name => 'DBIx-Fun',
    dist_version => "0.01",
    dist_author => 'Jeffery Klein',
    dist_abstract => 'DBIx::Fun - Call stored procedures without the fuss and muss.',
    license => 'gpl',
    requires => {
        perl => '5.8.2',
        DBI => 1.51
    },
    build_requires => {
        'Test::More' => 0.01
    }
);

$build->create_build_script();

