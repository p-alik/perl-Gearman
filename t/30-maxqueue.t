#!/usr/bin/perl

use strict;
use Gearman::Client;
use Storable qw( freeze );
use Test::More;

use lib 't';
use TestGearman;

# This is testing the MAXQUEUE feature of gearmand. There's no direct
# support for it in Gearman::Worker yet, so we connect directly to
# gearmand to configure it for the test.

if (start_server(PORT)) {
    plan tests => 6;
} else {
    plan skip_all => "Can't find server to test with";
    exit 0;
}

wait_for_port(PORT);

{
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => PORT,
    );
    ok($sock, "connect to jobserver");

    $sock->write( "MAXQUEUE long 1\n" );
    my $input = $sock->getline();
    ok($input =~ m/^OK\b/i);
}

start_worker(PORT);

my $client = Gearman::Client->new;
isa_ok($client, 'Gearman::Client');

$client->job_servers('127.0.0.1:' . PORT);

my $tasks = $client->new_task_set;
isa_ok($tasks, 'Gearman::Taskset');

my $failed = 0;
my $completed = 0;

foreach my $iter (1..5) {
    my $handle = $tasks->add_task('long', $iter, {
        on_complete => sub { $completed++ },
        on_fail => sub { $failed++ }
    });
}
$tasks->wait;

ok($completed == 2 || $completed == 1, 'number of success'); # One in the queue, plus one that may start immediately
ok($failed == 3 || $failed== 4, 'number of failure'); # All the rest



