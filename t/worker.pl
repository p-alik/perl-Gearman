#!/usr/bin/perl -w
use strict;

use Gearman::Worker;
use Storable qw( thaw );
use Getopt::Long qw( GetOptions );

GetOptions(
    's|servers=s', \my($servers),
);
die "usage: $0 -s <servers>" unless $servers;
my @servers = split /,/, $servers;

my $worker = Gearman::Worker->new;
$worker->job_servers(@servers);

$worker->register_function(sum => sub {
    my $sum = 0;
    $sum += $_ for @{ thaw($_[0]->arg) };
    $sum;
});

$worker->register_function(fail => sub { undef });
$worker->register_function(fail_exit => sub { exit 255 });

$worker->register_function(sleep => sub { sleep $_[0]->arg });

$worker->register_function(echo_ws => sub {
    select undef, undef, undef, 0.25;
    $_[0]->arg eq 'x' ? undef : $_[0]->arg;
});

$worker->register_function(long => sub {
    my($job) = @_;
    $job->set_status(50, 100);
    sleep 2;
    $job->set_status(100, 100);
    sleep 2;
});

$worker->work while 1;
