#!/usr/bin/perl

use strict;
use warnings;

use Gearman::Client;
use Storable qw( freeze );
use Test::More;
use Test::Timer;

use lib 't';
use TestGearman;

my $job_server;
{
    my $port = (free_ports(1))[0];
    if (!start_server($ENV{GEARMAND_PATH}, $port)) {
        plan skip_all => "Can't find server to test with";
        exit 0;
    }

    my $la = "127.0.0.1";
    $job_server = join ':', $la, $port;

    check_server_connection($job_server);
}

plan tests => 2;

start_worker([$job_server]);

my $client = new_ok("Gearman::Client", [job_servers => [$job_server]]);

subtest "wait with timeout", sub {
    ok(my $tasks = $client->new_task_set, "new_task_set");
    isa_ok($tasks, 'Gearman::Taskset');

    my ($iter, $completed, $failed, $handle) = (0, 0, 0);

    # handle => iter
    my %handles;

    my $opt = {
        uniq        => $iter,
        on_complete => sub {
            $completed++;
            delete $handles{$handle};
            note "Got result for $iter";
        },
        on_fail => sub {
            $failed++;
        },
    };

    # For a total of 5 events, that will be 20 seconds; till they complete.
    foreach $iter (1 .. 5) {
        ok($handle = $tasks->add_task('long', $iter, $opt),
            "add_task('long', $iter)");
        $handles{$handle} = $iter;
    }

    my $to = 11;
    time_ok(sub { $tasks->wait(timeout => $to) }, $to, "timeout");

    ok($completed > 0, "at least one job is completed");
    is($failed, 0, "no failed jobs");
};
