#!/usr/bin/perl

use strict;
use warnings;

use Gearman::Client;
use Storable qw( freeze );
use Test::More;
use lib 't';
use TestGearman;

# This is testing the MAXQUEUE feature of gearmand. There's no direct
# support for it in Gearman::Worker yet, so we connect directly to
# gearmand to configure it for the test.

my $job_server;
{
    my $port = (free_ports(1))[0];
    if (!start_server($ENV{GEARMAND_PATH}, $port)) {
        plan skip_all => "Can't find server to test with";
        exit 0;
    }

    plan tests => 6;

    my $la = "127.0.0.1";
    $job_server = join ':', $la, $port;

    check_server_connection($job_server);

    my $sock = IO::Socket::INET->new(
        PeerAddr => $la,
        PeerPort => $port,
    );
    ok($sock, "connect to jobserver");

    $sock->write("MAXQUEUE long 1\n");
    my $input = $sock->getline();
    ok($input =~ m/^OK\b/i);
}

start_worker([$job_server]);

my $client = new_ok("Gearman::Client", [job_servers => [$job_server]]);

my $tasks = $client->new_task_set;
isa_ok($tasks, 'Gearman::Taskset');

my $failed    = 0;
my $completed = 0;

foreach my $iter (1 .. 5) {
    my $handle = $tasks->add_task(
        'long', $iter,
        {
            on_complete => sub { $completed++ },
            on_fail     => sub { $failed++ }
        }
    );
} ## end foreach my $iter (1 .. 5)

$tasks->wait;

# One in the queue, plus one that may start immediately
ok($completed == 2 || $completed == 1, 'number of success');

# All the rest
ok($failed == 3 || $failed == 4, 'number of failure');

