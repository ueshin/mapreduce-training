#!/usr/bin/perl -w
use strict;

for my $line (<STDIN>) {
	for my $word (split /\W/, $line) {
		print "$word\t1\n" if($word ne '');
	}
}
