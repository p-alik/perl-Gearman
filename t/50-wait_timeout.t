#!/usr/bin/perl

use strict;
use Gearman::Client;
use Storable qw( freeze );
use Test::More;
use Time::HiRes qw(time);

use lib 't';
use TestGearman;

# This is testing the MAXQUEUE feature of gearmand. There's no direct
# support for it in Gearman::Worker yet, so we connect directly to
# gearmand to configure it for the test.

if (start_server(PORT)) {
    plan tests => 3;
} else {
    plan skip_all => "Can't find server to test with";
    exit 0;
}

wait_for_port(PORT);

start_worker(PORT);

my $client = Gearman::Client->new;
isa_ok($client, 'Gearman::Client');

$client->job_servers('127.0.0.1:' . PORT);

my $tasks = $client->new_task_set;
isa_ok($tasks, 'Gearman::Taskset');

my $failed = 0;
my $completed = 0;

my %handles; # handle => iter

# For a total of 5 events, that will be 20 seconds; till they complete.


foreach my $iter (1..5) {
    my $handle;
    $handle = $tasks->add_task('long', $iter, {
        uniq => $iter,
        on_complete => sub {
            $completed++;
            delete $handles{$handle};
            diag "Got result for $iter";
        },
        on_fail     => sub {
            $failed++
        },
    });
    $handles{$handle} = $iter;
}

$tasks->wait(timeout => 11);

my $late_tasks = $client->new_task_set;
isa_ok($tasks, 'Gearman::Taskset');


# vim: filetype=perl
