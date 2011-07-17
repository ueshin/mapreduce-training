#!/usr/bin/perl
use strict;
use warnings;

my $current = '';
my $count;
for my $keyvalue (<STDIN>) {
	chomp $keyvalue;
	my($key, $value) = split(/\t/, $keyvalue);
	if($key eq $current) {
		$count += $value;
	}
	else {
		if($current ne '') {
			print "$current\t$count\n";
		}
		$current = $key;
		$count = $value;
	}
}

print "$current\t$count\n";
