#!/usr/bin/perl -w
use strict;

my $current = '';
my $count;
for my $keyvalue (<STDIN>) {
	my($key, $value) = split(/\t/, $keyvalue);
	if($key eq $current) {
		$count++;
	}
	else {
		if($current ne '') {
			print "$current\t$count\n";
		}
		$current = $key;
		$count = 1;
	}
}
