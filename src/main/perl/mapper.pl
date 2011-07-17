#!/usr/bin/perl
use strict;
use warnings;

for my $line (<STDIN>) {
	chomp $line;
	for my $word (split /\W+/, $line) {
		print "$word\t1\n" if($word ne '');
	}
}
